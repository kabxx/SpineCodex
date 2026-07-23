use anyhow::Context as _;
use std::path::Path;
use std::path::PathBuf;
use tokio::process::Command;

const CODEX_WINDOWS_INSTALLER_URL: &str =
    "https://get.microsoft.com/installer/download/9PLM9XGG6VKS?cid=website_cta_psi";
const CODEX_MICROSOFT_STORE_WEB_URL: &str = "https://apps.microsoft.com/detail/9plm9xgg6vks";

pub async fn run_windows_app_open_or_install(
    workspace: PathBuf,
    download_url_override: Option<String>,
) -> anyhow::Result<()> {
    let display_workspace = display_workspace_path(&workspace);
    if codex_app_is_installed().await? {
        eprintln!("Opening Codex Desktop workspace {display_workspace}...");
        let spine_codex_bin = std::env::current_exe()
            .context("failed to resolve the current SpineCodex executable")?;
        open_codex_app(&codex_new_thread_url(&workspace), &spine_codex_bin).await?;
        return Ok(());
    }

    eprintln!("Codex Desktop not found; opening Windows installer...");
    let download_url = download_url_override
        .as_deref()
        .unwrap_or(CODEX_WINDOWS_INSTALLER_URL);
    if open_url(download_url).await.is_err() && download_url_override.is_none() {
        open_url(CODEX_MICROSOFT_STORE_WEB_URL).await?;
    }
    eprintln!("After installing Codex Desktop, open workspace {display_workspace}.");
    Ok(())
}

async fn codex_app_is_installed() -> anyhow::Result<bool> {
    let output = Command::new("powershell.exe")
        .arg("-NoProfile")
        .arg("-Command")
        .arg("(Get-AppxPackage -Name 'OpenAI.Codex' -ErrorAction SilentlyContinue).InstallLocation")
        .output()
        .await
        .context("failed to invoke `powershell.exe`")?;

    if !output.status.success() {
        return Ok(false);
    }

    Ok(!String::from_utf8_lossy(&output.stdout).trim().is_empty())
}

async fn open_codex_app(url: &str, spine_codex_bin: &Path) -> anyhow::Result<()> {
    let script = windows_desktop_app_launch_script(url, spine_codex_bin);
    let output = Command::new("powershell.exe")
        .arg("-NoProfile")
        .arg("-Command")
        .arg(&script)
        .output()
        .await
        .with_context(|| format!("failed to open {url}"))?;

    if output.status.success() {
        return Ok(());
    }

    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if stderr.is_empty() {
        anyhow::bail!("failed to open {url} with {}", output.status);
    }
    anyhow::bail!("{stderr}");
}

async fn open_url(url: &str) -> anyhow::Result<()> {
    let status = Command::new("powershell.exe")
        .arg("-NoProfile")
        .arg("-Command")
        .arg("& { param($target) Start-Process -FilePath $target }")
        .arg(url)
        .status()
        .await
        .with_context(|| format!("failed to open {url}"))?;

    if status.success() {
        return Ok(());
    }
    anyhow::bail!("failed to open {url} with {status}");
}

fn windows_desktop_app_launch_script(url: &str, spine_codex_bin: &Path) -> String {
    let url = powershell_single_quoted_string(url);
    let spine_codex_bin = powershell_single_quoted_string(&spine_codex_bin.display().to_string());
    format!(
        r#"
$ErrorActionPreference = 'Stop'
$url = {url}
$spineCodexBin = {spine_codex_bin}
$packages = @(Get-AppxPackage -Name 'OpenAI.Codex' -ErrorAction SilentlyContinue)
$package = $packages | Sort-Object Version -Descending | Select-Object -First 1
$installLocation = $package.InstallLocation
$packageFamilyName = $package.PackageFamilyName
if ([string]::IsNullOrWhiteSpace($installLocation) -or [string]::IsNullOrWhiteSpace($packageFamilyName)) {{
    Write-Error 'Codex Desktop package is not installed'
    exit 1
}}

$manifestPath = Join-Path $installLocation 'AppxManifest.xml'
if (-not (Test-Path $manifestPath)) {{
    Write-Error "Codex Desktop manifest not found at $manifestPath"
    exit 1
}}
$manifest = [xml](Get-Content -Raw -LiteralPath $manifestPath)
$application = $manifest.SelectSingleNode("/*[local-name()='Package']/*[local-name()='Applications']/*[local-name()='Application' and @Executable]")
if ($null -eq $application) {{
    Write-Error "Codex Desktop application entry not found in $manifestPath"
    exit 1
}}
$appId = $application.GetAttribute('Id')
$relativeExecutable = $application.GetAttribute('Executable')
$exe = Join-Path $installLocation ($relativeExecutable -replace '/', '\\')
$appDir = Split-Path -Parent $exe
if ([string]::IsNullOrWhiteSpace($appId) -or [string]::IsNullOrWhiteSpace($relativeExecutable) -or -not (Test-Path $exe)) {{
    Write-Error "Codex Desktop executable not found at $exe"
    exit 1
}}

$packageRoots = @($packages | ForEach-Object {{
    if (-not [string]::IsNullOrWhiteSpace($_.InstallLocation)) {{ $_.InstallLocation }}
}} | Where-Object {{ $_ }})
$processName = [System.IO.Path]::GetFileNameWithoutExtension($relativeExecutable)
$running = Get-Process -Name $processName,'Codex' -ErrorAction SilentlyContinue |
    Where-Object {{
        $processPath = $_.Path
        $processPath -and ($packageRoots | Where-Object {{
            $processPath.StartsWith($_, [System.StringComparison]::OrdinalIgnoreCase)
        }})
    }} |
    Select-Object -First 1
if ($null -ne $running) {{
    Write-Error "Codex Desktop is already running (PID $($running.Id)); quit it completely, then rerun 'spine-codex app' so CODEX_CLI_PATH can be applied"
    exit 1
}}

$innerScript = @'
function Decode-Value([string]$value) {{
    [System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String($value))
}}
$ErrorActionPreference = 'Stop'
$env:CODEX_CLI_PATH = Decode-Value '__SPINE_CODEX_BIN__'
Start-Process -FilePath (Decode-Value '__CODEX_APP_EXE__') -WorkingDirectory (Decode-Value '__CODEX_APP_DIR__') -ArgumentList @((Decode-Value '__CODEX_APP_URL__'))
'@
function Encode-Value([string]$value) {{
    [System.Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($value))
}}
$innerScript = $innerScript.Replace('__SPINE_CODEX_BIN__', (Encode-Value $spineCodexBin))
$innerScript = $innerScript.Replace('__CODEX_APP_EXE__', (Encode-Value $exe))
$innerScript = $innerScript.Replace('__CODEX_APP_DIR__', (Encode-Value $appDir))
$innerScript = $innerScript.Replace('__CODEX_APP_URL__', (Encode-Value $url))
$encodedScript = [System.Convert]::ToBase64String([System.Text.Encoding]::Unicode.GetBytes($innerScript))
$innerArgs = '-NoProfile -NonInteractive -EncodedCommand ' + $encodedScript
Invoke-CommandInDesktopPackage -PackageFamilyName $packageFamilyName -AppId $appId -Command 'powershell.exe' -Args $innerArgs
"#
    )
}

fn powershell_single_quoted_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn codex_new_thread_url(workspace: &Path) -> String {
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    serializer.append_pair("path", &workspace.display().to_string());
    let query = serializer.finish();
    format!("codex://threads/new?{query}")
}

fn display_workspace_path(workspace: &Path) -> String {
    let path = workspace.display().to_string();
    if let Some(path) = path.strip_prefix(r"\\?\UNC\") {
        format!(r"\\{path}")
    } else if let Some(path) = path.strip_prefix(r"\\?\") {
        path.to_string()
    } else {
        path
    }
}

#[cfg(test)]
mod tests {
    use super::codex_new_thread_url;
    use super::display_workspace_path;
    use super::powershell_single_quoted_string;
    use super::windows_desktop_app_launch_script;
    use pretty_assertions::assert_eq;
    use std::path::Path;

    #[test]
    fn display_workspace_path_removes_windows_extended_prefix() {
        assert_eq!(
            display_workspace_path(Path::new(r"\\?\C:\Users\fcoury\code\codex")),
            r"C:\Users\fcoury\code\codex"
        );
    }

    #[test]
    fn display_workspace_path_preserves_unc_prefix() {
        assert_eq!(
            display_workspace_path(Path::new(r"\\?\UNC\server\share\codex")),
            r"\\server\share\codex"
        );
    }

    #[test]
    fn display_workspace_path_leaves_regular_paths_unchanged() {
        assert_eq!(
            display_workspace_path(Path::new(r"C:\Users\fcoury\code\codex")),
            r"C:\Users\fcoury\code\codex"
        );
    }

    #[test]
    fn codex_new_thread_url_encodes_windows_workspace_path() {
        assert_eq!(
            codex_new_thread_url(Path::new(r"C:\Users\akuma\repos\koba")),
            r"codex://threads/new?path=C%3A%5CUsers%5Cakuma%5Crepos%5Ckoba"
        );
    }

    #[test]
    fn codex_new_thread_url_preserves_verbatim_workspace_path() {
        assert_eq!(
            codex_new_thread_url(Path::new(r"\\?\C:\Users\akuma\repos\koba")),
            r"codex://threads/new?path=%5C%5C%3F%5CC%3A%5CUsers%5Cakuma%5Crepos%5Ckoba"
        );
    }

    #[test]
    fn powershell_single_quoted_string_escapes_quotes() {
        assert_eq!(
            powershell_single_quoted_string("C:\\Users\\O'Neil"),
            "'C:\\Users\\O''Neil'"
        );
    }

    #[test]
    fn launch_script_injects_spine_codex_and_deep_link() {
        let script = windows_desktop_app_launch_script(
            "codex://threads/new?path=C%3A%5Cworkspace",
            Path::new(r"C:\Users\me\spine codex.exe"),
        );

        assert!(script.contains("$env:CODEX_CLI_PATH = Decode-Value"));
        assert!(script.contains("'C:\\Users\\me\\spine codex.exe'"));
        assert!(script.contains("'codex://threads/new?path=C%3A%5Cworkspace'"));
        assert!(script.contains("Get-AppxPackage -Name 'OpenAI.Codex'"));
        assert!(script.contains("AppxManifest.xml"));
        assert!(script.contains("GetAttribute('Id')"));
        assert!(script.contains("GetAttribute('Executable')"));
        assert!(script.contains("$packageRoots = @($packages | ForEach-Object"));
        assert!(script.contains("$processName = [System.IO.Path]::GetFileNameWithoutExtension"));
        assert!(script.contains("Invoke-CommandInDesktopPackage"));
        assert!(script.contains("-AppId $appId"));
        assert!(script.contains("-EncodedCommand"));
        assert!(!script.contains("app.asar"));
        assert!(!script.contains("ChatGPT.exe"));
    }
}
