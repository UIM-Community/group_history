package Lib::robot;
use strict;
use warnings;
our $DB;

sub new {
    my ($class,$robotname,$cs_id) = @_;
    my @Array = ();
    my $this = {
        cs_id => $cs_id,
        robotname => $robotname,
        groups => \@Array
    };
    return bless($this,ref($class) || $class);
}

sub set {
    my ($self,$groupName) = @_; 
    push($self->{groups},$groupName);
}

sub triggerInactive {
    my ($self,$Logger,$STR_TableName) = @_;
    my ($sth,$update);
    $Logger->info("---------------------------------------");
    $Logger->info("Trigger Inactive for $self->{robotname}");
    my $sch = "";
    foreach my $w (@{ $self->{groups} }) {
        $sch .= "'$w',";
        $Logger->info("Group => $w");
    }
    chop $sch;

    my $SQLQuery = "SELECT groupname FROM $STR_TableName WITH (NOLOCK) WHERE cs_id = $self->{cs_id} AND robotname = '$self->{robotname}' AND groupname NOT IN ($sch)";
    $Logger->info($SQLQuery);
    $sth = $DB->prepare($SQLQuery);
    $sth->execute;
    my $rows = 0;

    my @RawRef = ();
    while(my $hashRef = $sth->fetchrow_hashref) {
        push(@RawRef,$hashRef);
        $rows++;
    }
    $sth->finish;

    if($rows > 0) {
        $DB->begin_work;
        foreach my $dbRef (@RawRef) {
            $update = $DB->prepare("UPDATE $STR_TableName SET active=0,deleted=GETDATE() WHERE cs_id = ? AND robotname = ? and groupname = ?");
            $update->execute($self->{cs_id},$self->{robotname},$dbRef->{groupname});
            $Logger->info("UPDATE state for $dbRef->{groupname}, active=0 and deleted = NOW()");
            $update->finish;
        }
        $DB->commit;
    }
}

1;
