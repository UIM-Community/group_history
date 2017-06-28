use strict;
use warnings;
use lib "D:/apps/Nimsoft/perllib";
use lib "D:/apps/Nimsoft/Perl64/lib/Win32API";
use DBI;
use Nimbus::API;
use Nimbus::CFG;
use Nimbus::PDS;
use Perluim::API;
use Perluim::Addons::CFGManager;
use Perluim::Core::Events;
use Lib::robot;

# Global variables
$Perluim::API::Debug = 1;
my ($STR_Properties,$STR_Login,$STR_Password,$INT_Interval,$INT_MaxSQLHistory,$STR_TableName);
my $Probe_NAME  = "group_history";
my $Probe_VER   = "1.0";
my $Probe_CFG   = "group_history.cfg";
$SIG{__DIE__} = \&scriptDieHandler;

#
# Register logger!
# 
my $Logger = uimLogger({
    file => "group_history.log",
    level => 6
});

#
# scriptDieHandler
#
sub scriptDieHandler {
    my ($err) = @_; 
    $Logger->fatal($err);
    exit(1);
}

#
# Init and configuration configuration
#
sub read_configuration {
    $Logger->info("Read and parse configuration file!");

    my $CFGManager = Perluim::Addons::CFGManager->new($Probe_CFG,1);
    $Logger->trace( $CFGManager );

    $STR_Login               = $CFGManager->read("setup","login","administrator");
    $STR_Password            = $CFGManager->read("setup","password");
    $STR_TableName           = $CFGManager->read("setup","tablename","ssr_groups_history");
    $INT_Interval            = $CFGManager->read("setup","interval",10800000);
    $INT_MaxSQLHistory       = $CFGManager->read("setup","max_sql_history",90);

    createDirectory("output");
}
read_configuration();

# Login to Nimbus!
nimLogin("$STR_Login","$STR_Password") if defined $STR_Login && defined $STR_Password;

#
# Register probe
# 
my $probe = uimProbe({
    name    => $Probe_NAME,
    version => $Probe_VER,
    timeout => $INT_Interval
});
$Logger->trace( $probe );

# Register callbacks (String and Int are valid type for arguments)
$probe->registerCallback( "get_info" );
$probe->registerCallback( "scan" );

# Probe restarted
$probe->on( restart => sub {
    $Logger->info("Probe restarted");
    read_configuration();
});

# Probe timeout
$probe->on( timeout => sub {
    $Logger->info("Probe timeout");
    main();
});

# Start probe!
main();
$probe->start();

#
# connect database 
#
sub connect_db {
    my $CFGManager = Perluim::Addons::CFGManager->new($Probe_CFG,1);

    my $DB_User         = $CFGManager->read("CMDB","sql_user");
    my $DB_Password     = $CFGManager->read("CMDB","sql_password");
    my $DB_SQLServer    = $CFGManager->read("CMDB","sql_host");
    my $DB_Database     = $CFGManager->read("CMDB","sql_database");
    $Logger->info("Try to connect to $DB_SQLServer");

    my $DB = DBI->connect("$DB_SQLServer;UID=$DB_User;PWD=$DB_Password",{
        RaiseError => 0,
        AutoCommit => 1,
        PrintError => 0
    });

    $DB->do("USE CA_UIM");
    return $DB;
}

#
# Main method (called in timeout callback of the probe).
#
sub main {
    $Logger->info("Timeout executed!");
    my $DB = connect_db();

    # Get all robots with groups!
    $Logger->info("Get robots groups (state right now)");
    my $sth = $DB->prepare("SELECT CS.cs_id,CS.name as robotname,CG.name as groupname FROM cm_computer_system AS CS WITH (NOLOCK) INNER JOIN cm_group_member AS GM WITH (NOLOCK) ON GM.cs_id = CS.cs_id INNER JOIN cm_group AS CG WITH (NOLOCK) ON GM.grp_id = CG.grp_id");
    $sth->execute;
    my @Ret = ();
    while(my $hashRef = $sth->fetchrow_hashref) {
        push(@Ret,$hashRef);
    }
    $sth->finish;

    # Use CMDB_Import 
    $DB->do("USE CMDB_Import");

    # Delete old
    $Logger->info("Delete old entry!");
    my $delete_old = $DB->prepare("DELETE FROM $STR_TableName WHERE created < DATEADD(day, -$INT_MaxSQLHistory, GETDATE())");
    $delete_old->execute;
    $delete_old->finish;
    undef $delete_old;

    my %Robots = ();
    $Lib::robot::DB = $DB;

    # Hydrate hash collection!
    foreach my $dbRef (@Ret) {
        my $cs_id       = $dbRef->{cs_id};
        my $robotname   = $dbRef->{robotname};
        my $groupname   = $dbRef->{groupname};
        my ($insert,$update,$gtn);

        $gtn = $DB->prepare("SELECT active FROM $STR_TableName WITH (NOLOCK) WHERE cs_id = ? AND robotname = ? AND groupname = ?");
        $gtn->execute($cs_id,$robotname,$groupname);
        my $rows = 0;

        my @RawRef = ();
        while(my $hashRef = $gtn->fetchrow_hashref) {
            $rows++;
            push(@RawRef,$hashRef);
        }
        $gtn->finish;

        $Logger->info(":: cs_id => $cs_id , robotname => $robotname, groupname => $groupname [row $rows]");
        
        $DB->begin_work;
        if($rows > 0) {
            foreach my $hashRef (@RawRef) {
                if($hashRef->{active} == 0) {
                    $Logger->info("Update $robotname in group $groupname, set active = 1");
                    $update = $DB->prepare("UPDATE $STR_TableName SET active=1, updated=GETDATE() WHERE cs_id = ? AND robotname = ? AND groupname = ?");
                    $update->execute($cs_id,$robotname,$groupname);
                    $update->finish;
                }
                last;
            }
        }
        else {
            my $insert = $DB->prepare("INSERT INTO $STR_TableName (cs_id,robotname,groupname) VALUES(?,?,?)");
            $insert->execute($cs_id,$robotname,$groupname);
            $insert->finish;
        }
        $DB->commit;

        if(exists($Robots{$robotname})) {
            $Robots{$robotname}->set($groupname);
        }
        else {
            $Robots{$robotname} = Lib::robot->new($robotname,$cs_id);
            $Robots{$robotname}->set($groupname);
        }
    }

    # Detect when robot are in or out!
    foreach my $robotName (keys %Robots) {
        $Robots{$robotName}->triggerInactive($Logger,$STR_TableName);
    }

    # Close database!
    $Logger->info("Disconnect database!");
    $DB->disconnect;

    # Copy log file in ouput directory!
    eval {
        my $T = getDate();
        createDirectory("output/$T");
        $Logger->copyTo("output/$T");
    };
    if($@) {
        $Logger->error("Failed to copy logfile!");
    }
}

#
# get_info callback!
#
sub get_info {
    my ($hMsg) = @_;
    $Logger->info("get_info callback triggered !");
    nimSendReply($hMsg,NIME_OK);
}

#
# Scan callback
#
sub scan {
    my ($hMsg) = @_;
    $Logger->info("Scan callback triggered !");
    nimSendReply($hMsg,NIME_OK);
    main();
}
