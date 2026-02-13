function test-branch() {
    <#
    .SYNOPSIS
        Will check and error stop if fails that commit on HEAD (working copy)
        matches with latest commit on remote branch. This is used to ensure
        that regression test is being run against correct commit.
        If the two match then no output is returned

    .DESCRIPTION
        Primarily this is designed as a check to run at the beginning of PROD
        regression.

    .EXAMPLE
        test-branch -branch "origin/main"
    #>

    # this is needed to support verbose arg
    [CmdletBinding()]
    param (
        $branch = "origin/main"
    )


    $remoteCommit=git rev-parse --verify $branch
    if ( $LASTEXITCODE -ne 0 ) {
        throw "Branch does not exist: $($branch)"
    }
    $localCommit=git rev-parse HEAD

    if ( $remoteCommit -ne $localCommit ) {
        $errorRes="Regression is being run against different version from committed on remote $($branch)"
        $errorRes+="`nremoteCommit: $($remoteCommit)"
        $errorRes+="`nlocalCommit:  $($localCommit)"
        $errorRes+="`n "
        throw $errorRes
    }

    Write-Verbose "remoteCommit: $($remoteCommit) ($($branch))"
    Write-Verbose "localCommit:  $($localCommit) (HEAD)"

}