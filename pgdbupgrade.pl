#! /usr/bin/perl

# OpenGeo Suite PostGIS database upgrade script
# Automates the data loading process from OpenGeo Suite 2.5 to 3.0
# (PostGIS 1.5 to PostGIS 2.0)

use DBI;
package DBD::pg;

use warnings;
use strict;

# -------------------------
# DBI Graveyard
#TODO: None of this has worked yet

#my $dbh = DBI->connect("dbi:Pg:dbname=medford;host=localhost;port=54321",'postgres','',{AutoCommit => 0, RaiseError => 1, PrintError => 0});

#print "3+3=",$dbh->selectrow_array("SELECT 3+3"),"\n";

#@dblist = $dbh->data_sources("port=54321");


#my @dblist = $dbh->execute("SELECT datname FROM pg_database WHERE datistemplate IS FALSE and datname NOT LIKE 'postgres'");
#@dblist = $dbh->data_sources();

# -------------------------


my $me = $0;

# Usage
my $usage = qq{
Usage:	$me [--backup|--restore] <dumppath>
        Creates a backup of an OpenGeo Suite 2.x database system.
        Restores this backup for use in an OpenGeo Suite 3.x database system. 
        <dumppath> = location to save backup files
};

#TODO: Check for postgis_restore.pl
#TODO: Include postgis_restore.pl inside script?  How?

#TODO: Silent running of these programs
die "$me:\tUnable to find 'pg_dump' on the path.\n" if ! `pg_dump --version`;
die "$me:\tUnable to find 'pg_dumpall' on the path.\n" if ! `pg_dumpall --version`;
die "$me:\tUnable to find 'pg_restore' on the path.\n" if ! `pg_restore --version`;
die "$me:\tUnable to find 'createdb' on the path.\n" if ! `createdb --version`;
die "$me:\tUnable to find 'psql' on the path.\n" if ! `psql --version`;

# Check for proper arguments
die $usage if (@ARGV != 2);

my $operation = $ARGV[0];
my $dumppath = $ARGV[1];

#TODO: Do we rely on env vars for port, username, password, etc
#      Or do we pass these?
#      Assuming we use env vars now

#TODO: Fail if this command fails
my $psqlcheck = `psql -t -A -c "SELECT postgis_version()"`;
if (not $psqlcheck) {
  print "Can't connect to database.  Please check connections.\n";
  exit;
}

my @pgver = split(/ /,"$psqlcheck");
my $pgver = $pgver[0];

# Check that $dumppath exists
#TODO: Sanitize $dumppath to account for paths with spaces
#      Replace spaces with escaped spaces?
#      May already be working
#TODO:Check for write permissions on $dumppath
if (not -d $dumppath) {
  print "Error: $dumppath doesn't exist";
  die $usage;
}


# Do it!
my $result;
if (($operation eq "-b") || ($operation eq "--backup")) {
  $result = backup($dumppath, $pgver);
}
if (($operation eq "-r") || ($operation eq "--restore")) {
  $result = restore($dumppath, $pgver);
}

# Bad $operation will have no $result
if (!defined($result)) {
  die $usage;
}

print "Operation complete";
exit;

# End



# ------
# Backup
# ------

sub backup {

  # Check for PostGIS 1.x
  if (substr($pgver, 0, 1) != 1) {
    die "$me:\tPostGIS 1.x required for this operation.\n";
  }
  print "PostGIS version $pgver found.\n";

  print "Backing up databases to $dumppath\n";

  # Get a list of all relevant databases
  #TODO: How to exclude non-spatial DBs?
  my @dblist = `psql -t -A -d postgres --command "SELECT datname FROM pg_database WHERE datistemplate IS FALSE and datname NOT LIKE 'postgres';"`;

  # Total number of databases found
  my $dbtot = scalar @dblist;
  my $count;
  print "Found the following $dbtot databases:\n {";
  for ($count = 0; $count < $dbtot; $count++) {
    chomp($dblist[$count]);
    print "$dblist[$count] ";
  }
  print "}\n";

  # dump each database to disk
  #TODO: Suppress ftell mismatch warning
  for my $db (@dblist) {
    print "Dumping: $db\n";
    my $dbdump = `pg_dump -Fc $db`;
    open (MYFILE, ">$dumppath/$db.dmp");
    print MYFILE $dbdump;
    close (MYFILE);
  }

  # dump the database roles
  print "Dumping: roles\n";
  my $dbroledump = `pg_dumpall -r`;
  open (MYFILE, ">$dumppath/roles.sql");
  print MYFILE $dbroledump;
  close (MYFILE);

  # Summary
  #TODO: Verify this list?
  print "\nCreated the following files in \"$dumppath\":\n";
  for ($count = 0; $count < $dbtot; $count++) {
    chomp($dblist[$count]);
    print " $dblist[$count].dmp\n";
  }
  print " roles.sql\n";

}


# -------
# Restore
# -------

sub restore {

  # Check for PostGIS 2.x
  if (substr($pgver, 0, 1) != 2) {
    die "$me:\tPostGIS 2.x required for this operation.\n";
  }

  print "Restoring databases from $dumppath\n";

  # Restore the roles
  my $dbrolerestore = `psql $dumppath/roles.sql`;

  # Find all the dump files
  opendir my $dir, $dumppath or die "Cannot open directory: $!";
  my @dmpfiles = grep { -f && /\.dmp$/ } readdir $dir;
  closedir $dumppath;
  print "Found the following files:\n";
  for my $file (@dmpfiles) {
    print "$file\n";
  }

  # Strip off the ".dmp"
  my @newdblist;
  for my $file (@dmpfiles) {
    my $noextfile = substr($file, 0, -4);
    push(@newdblist, $noextfile);
  }

  # Create, convert, and load the new DBs
  for my $newdb (@newdblist) {
    print "Restoring database: $newdb\n";
    print "Creating new database in system:\n";
    my $createdb = `createdb $newdb`;
    #TODO: "Useless use of a constant (.dmp) in void context" why?
    my $createpg = `psql -t -A -d $newdb -c "create extension postgis"`;
    my $newdbfile = $newdb,".dmp";
    print "Coverting $newdbfile to PostGIS 2.0 format:\n";
    my $convert = `postgis_restore.pl $newdbfile`;
    print "Loading into PostGIS 2.0:\n";
    my $psql = `psql $newdb $newdbfile`;
    print "Restore of database $newdb complete\n\n";
  }

}