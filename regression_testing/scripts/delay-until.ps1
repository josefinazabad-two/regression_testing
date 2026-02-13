function delay-until() {
    <#
    .SYNOPSIS
    delays execution until a specified time
    #>
    param (
        $date = $null
    )

    if ( $date -ne $null ) {
        $delayUntil = (get-date -date "$($date)")
        $now=(get-date)

        $delaySeconds = ($delayUntil - $now).TotalSeconds
        bii-log-message "Delay Until date = $($date), now = $($now), delaySeconds=$($delaySeconds)"

        if ( $delaySeconds -gt 0 ) {
            sleep -seconds $delaySeconds
        }
    }
}

### End of File
