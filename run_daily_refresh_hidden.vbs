Set shell = CreateObject("WScript.Shell")
scriptDir = CreateObject("Scripting.FileSystemObject").GetParentFolderName(WScript.ScriptFullName)
command = "cmd.exe /c """ & scriptDir & "\run_daily_refresh.bat"""
shell.Run command, 0, True
