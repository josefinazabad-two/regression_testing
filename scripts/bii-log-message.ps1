#######################################################################
#
#   Filename:   bii-log-message.ps1
#   Author:     Jared Church
#
#   Purpose:
#       Standardise formatting of log messages, including timestamps
#
#######################################################################


function bii-log-message2() {
    <#
    .SYNOPSIS
        Standardise log message output formatting.
    .DESCRIPTION
        control of the output should be done in the script itself whether
        the output should be warning, debug etc
    .NOTES
        None
    .LINK
        None
    .EXAMPLE
    #>
    param (
        [string]$message
    )

    "[$(get-date -Format "dd/MMM/yyyy HH:mm:ss")] $($message)"
}


function bii-log-message() {
    <#
    .SYNOPSIS
        Standardise log message output formatting.
    .DESCRIPTION
        At this stage this is only info logging, in future perhaps expand to allow terminal and non-terminal errors
        defines 4 logging levels: error, warning, info, debug
        default is to output errors, warnings and info, but not debug
        increase logLevel to 4 to output debug as well

        this does not handle any termination of process or generation of actual error messages in powershell - this
        should probably be handled in the future.
        Also probably in the future allow the log level to be set by an environment variable
    .NOTES
        None
    .LINK
        None
    .EXAMPLE
    #>
    param (
        $message,
        $logType = "INFO",
        [switch] $logDebug,
        [switch] $logWarning,
        [switch] $logError,
        $logLevel = 3
    )

    write-debug "bii-log-message is deprecated, please stop using it 17/Mar/2025"

    $cmd="write-output"

    if ( $logDebug ) {
        $logType = 'DEBUG'
        $cmpLogLevel=4
    } elseif ( $logWarning ) {
        $logType = 'WARNING'
        $cmpLogLevel=2
        $cmd="write-warning"
    } elseif ( $logError ) {
        $logType = 'ERROR'
        $cmpLogLevel=1
        $cmd="write-error"
    }

    $logOutput="[$(get-date -Format "dd/MMM/yyyy HH:mm:ss")] $($logType): $($message)"
    if ( $logLevel -ge $cmpLogLevel ) {
        & $cmd $logOutput
    }
}


### End of File
