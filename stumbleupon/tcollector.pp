# Example Puppet manifest for updating/starting tcollector
# under puppet

class tcollector {
    package { python:
        ensure => installed,
    }

    service { tcollector:
        ensure => running,
        require => [Package["python"], File["/usr/local/tcollector"]],
        start => "/usr/local/tcollector/startstop start",
        stop => "/usr/local/tcollector/startstop stop",
        restart => "/usr/local/tcollector/startstop restart",
        status => "/usr/local/tcollector/startstop status",
        subscribe => File["/usr/local/tcollector"],
    }

    file { ["/usr/local"]:
        owner  => root, group => root, mode => 755,
        ensure => directory,
    }

    file { "/usr/local/tcollector":
        source  => "puppet:///files/tcollector",
        owner   => root, group => root,
        ensure => directory,
        recurse => true,
        ignore => '*.pyc',
        purge => true,
        force => true,
        require => File["/usr/local"],
    }
}
