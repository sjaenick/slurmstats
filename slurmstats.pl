#!/usr/bin/env perl

use strict;
use warnings;

use Getopt::Std;
use Data::Dumper;

my $VERSION = "1.00";

my %opts;
getopts('ahj:k', \%opts);

my $array_id = $opts{'j'};

usage() unless defined($array_id) and ($array_id =~ /^\d+$/);
usage() if defined($opts{'h'}) and defined($opts{'k'});

sub usage {
    print STDERR "slurmstats $VERSION\n\n";
    print STDERR "Usage: $0 [-a] [-h | -k] -j arrayId\n";
    print STDERR "    -a  Display average time instead of median\n";
    print STDERR "    -h  Group jobs by host\n";
    print STDERR "    -k  Group jobs by kernel version\n";
    exit(1);
}


my $host2kernel = {}; # cache for kernel versions

sub get_host_kernel {
    my $host = shift;
    return $host2kernel->{$host} if (defined($host2kernel->{$host}));

    my $kernel = get_host_kernel_scontrol($host);
    $host2kernel->{$host} = $kernel;
    return $kernel;
}

sub get_host_kernel_scontrol {
    my $host = shift;
    my $kernel;
    open(CMD, "scontrol show node $host |") or die $!;
    foreach my $line (<CMD>) {
        if ($line =~ /^\s+OS=/) {
            my @fields = split(/\s+/, $line);
            $kernel = $fields[2];
        }
    }
    close(CMD);
    return $kernel;
}

sub get_host_kernel_ssh {
    my $host = shift;
    open(SSH, "ssh $host uname -r |") or die $!;
    my $kernel = <SSH>;
    chomp($kernel);
    close(SSH);

    return $kernel;
}

sub get_user_role {
    my $login = getpwuid($<);
    open(CMD, "sacctmgr show user $login format=admin |") or die $!;
    my $line = <CMD>;
    close(CMD);
    chomp($line);
    return $line;
}

sub median {
    my @sorted = sort { $a <=> $b } @_;
    my $len = @sorted;
    my $ret;
    if ($len % 2) {
        return $sorted[int($len/2)];
    } else {
        return ($sorted[int($len/2)-1] + $sorted[int($len/2)])/2;
    }
}

sub average {
    my $len = @_;
    my $sum = 0;
    map { $sum += $_ } @_;
    return $sum/$len;
}

sub parse_next_job {
    my $fh = shift;

    my $data = {};

    my $unused = <$fh>;
    my $line = <$fh>;

    return undef unless defined($line);

    #print STDERR $line;

    chomp($line);
    chop($line); # remove trailing |

    my ($task, $state, $runtime, $elapsed, $host) = split(/\|/, $line);

    $task = substr($task, 0, length($task) - 6); # remove trailing ".batch"
    my ($job_id, $job_idx) = ($task =~ /(\d+)_(\d+)/);
    $data->{'jobid'} = $job_id;
    $data->{'jobidx'} = $job_idx;

    $data->{'state'} = $state;

    my @splitted = split(/[-:]+/, $runtime);
    if (scalar(@splitted) == 4) {
        my ($dd, $hh, $mm, $ss) = @splitted;
        my $seconds = $dd*86400 + $hh*3600 + $mm*60 + $ss;
        $data->{'runtime'} = $seconds;
    } elsif (scalar(@splitted) == 3) {
        my ($hh, $mm, $ss) = @splitted;
        my $seconds = $hh*3600 + $mm*60 + $ss;
        $data->{'runtime'} = $seconds;
    } else {
        die "Invalid duration: $runtime";
    }
    

    @splitted = split(/[-:]+/, $elapsed);
    if (scalar(@splitted) == 4) {
        my ($dd, $hh, $mm, $ss) = @splitted;
        my $seconds = $dd*86400 + $hh*3600 + $mm*60 + $ss;
        $data->{'wallclock'} = $seconds;
    } elsif (scalar(@splitted) == 3) {
        my ($hh, $mm, $ss) = @splitted;
        my $seconds = $hh*3600 + $mm*60 + $ss;
        $data->{'wallclock'} = $seconds;
    } else {
        die "Invalid duration: $elapsed";
    }


    $data->{'host'} = $host;

    return $data;
}

my $criteria = 'all';
$criteria = 'host' if defined($opts{"h"});
$criteria = 'kernel' if defined($opts{"k"});
my $criteria2runtimes = {};
my $criteria2wallclocks = {};

my $strategy = defined($opts{"a"}) ? 'average' : 'median';

#my $userrole = get_user_role();


open(my $fh, "sacct -j $array_id -o JobID,State,AveCPU,Elapsed,MaxRSSNode -p -n -s cd |") or die $!;
while (my $entry = parse_next_job($fh)) {

    #print Dumper \$entry;

    next if ($entry->{'state'} ne 'COMPLETED');

    my $host = $entry->{'host'};
    my $runtime = $entry->{'runtime'};
    my $wallclock = $entry->{'wallclock'};

    my $key = 'all';
    if ($criteria eq 'host') {
        $key = $entry->{'host'};
    } elsif ($criteria eq 'kernel') {
        $key = get_host_kernel($entry->{'host'});
    } else {
        die "Unknown criteria: $criteria";
    }


    if (defined($criteria2runtimes->{$key})) {
        push @{$criteria2runtimes->{$key}}, $runtime;
    } else {
        $criteria2runtimes->{$key} = [ $runtime ];
    }

    if (defined($criteria2wallclocks->{$key})) {
        push @{$criteria2wallclocks->{$key}}, $wallclock;
    } else {
        $criteria2wallclocks->{$key} = [ $wallclock ];
    }
}
close($fh);

print $criteria, "\tnum_tasks\t", $strategy, "_runtime\t", $strategy, "_wallclock\n";

foreach my $crit (sort { $a cmp $b } keys %$criteria2runtimes) {
    my $runtimes = $criteria2runtimes->{$crit};
    my $wallclocks = $criteria2wallclocks->{$crit};
    my $num = scalar(@$runtimes);

    if ($strategy eq 'median') {
        my $medianruntime = median(@$runtimes);
        my $medianwallclock = median(@$wallclocks);
        printf("%s\t%d\t%10.2f\t%10.2f\n", $crit, $num, $medianruntime, $medianwallclock);
    } elsif ($strategy eq 'average') {
        my $avgruntime = average(@$runtimes);
        my $avgwallclock = average(@$wallclocks);
        printf("%s\t%d\t%10.2f\t%10.2f\n", $crit, $num, $avgruntime, $avgwallclock);
    } else {
        die;
    }

}
