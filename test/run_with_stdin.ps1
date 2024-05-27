param (
    [string[]]$args
)

# Read all input from stdin
$inputData = [Console]::In.ReadToEnd()

# Construct the arguments for the Python module
$pythonArgs = $args -join ' '

# Construct the command to run the Python module with stdin redirection
$command = "python -m avrotize $pythonArgs"

# Start the Python process
$process = Start-Process -FilePath "python" -ArgumentList "-m avrotize $pythonArgs" -NoNewWindow -PassThru -RedirectStandardInput "input.txt"

# Write the stdin data to the process
$process.StandardInput.WriteLine($inputData)
$process.StandardInput.Close()

# Wait for the process to exit
$process.WaitForExit()

# Output the exit code of the process
exit $process.ExitCode
