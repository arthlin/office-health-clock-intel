Set objShell = CreateObject("Shell.Application")
Dim appDir
appDir = CreateObject("Scripting.FileSystemObject").GetParentFolderName(WScript.ScriptFullName)

Dim pythonw
pythonw = appDir & "\.venv\Scripts\pythonw.exe"

objShell.ShellExecute pythonw, """" & appDir & "\main.py""", appDir, "runas", 1
