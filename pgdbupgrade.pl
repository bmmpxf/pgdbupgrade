﻿#! /usr/bin/perl

# OpenGeo Suite PostGIS database upgrade script
# Automates the data loading process from OpenGeo Suite 2.5 to 3.0
# (PostGIS 1.5 to PostGIS 2.0)

use warnings;
use strict;

# -------------------------
# DBI Graveyard

#TODO: None of this has worked yet

#use DBI;
#package DBD::pg;


#my $dbh = DBI->connect("dbi:Pg:dbname=medford;host=localhost;port=54321",'postgres','',{AutoCommit => 0, RaiseError => 1, PrintError => 0});

#print "3+3=",$dbh->selectrow_array("SELECT 3+3"),"\n";

#@dblist = $dbh->data_sources("port=54321");


#my @dblist = $dbh->execute("SELECT datname FROM pg_database WHERE datistemplate IS FALSE and datname NOT LIKE 'postgres'");
#@dblist = $dbh->data_sources();

# -------------------------


my $me = $0;

# Usage
my $usage = qq{
Usage:	$me [--backup|--restore] [dumppath]
        Creates a backup of an OpenGeo Suite 2.x database system.
        Restores this backup for use in an OpenGeo Suite 3.x database system. 
        [dumppath] = location to save backup files, or current directory.
        Uses default connection parameters. Please ensure that
        PGHOST, PGUSER, PGPORT, PGPASSWORD are set correctly.
};

print "OpenGeo Suite PostGIS backup/restore utility.\n";

my $os = $^O; # MSWin32 for Windows

#TODO: Silent running of these programs for failure
die "$me:\tUnable to find 'pg_dump' on the path.\n" if ! `pg_dump --version`;
die "$me:\tUnable to find 'pg_dumpall' on the path.\n" if ! `pg_dumpall --version`;
die "$me:\tUnable to find 'pg_restore' on the path.\n" if ! `pg_restore --version`;
die "$me:\tUnable to find 'createdb' on the path.\n" if ! `createdb --version`;
die "$me:\tUnable to find 'psql' on the path.\n" if ! `psql --version`;

# Check for proper arguments
die $usage if ((@ARGV > 2) || (@ARGV == 0));

# If dumppath not specified, using current directory
my $operation = $ARGV[0];
my $dumppath;
if (@ARGV == 1) {
  print "Dumppath not specified, using current directory.\n";
  $dumppath = ".";
} else {
  $dumppath = $ARGV[1];
}

# Check that $dumppath exists
if (not -d $dumppath) {
  die "Path \"$dumppath\" doesn't exist.\n";
}

# Check for write permissions on $dumppath
#TODO: Better way to do this?
open (TEST, ">$dumppath/tmp");
close (TEST) || die "Could not write to \"$dumppath\".  Please check permissions.\n";
unlink("$dumppath/tmp"); # Clean up;

# Do it!
my $result;
if (($operation eq "-b") || ($operation eq "--backup")) {
  $result = backup($dumppath);
}
if (($operation eq "-r") || ($operation eq "--restore")) {
  $result = restore($dumppath);
}

# Bad $operation will have no $result
if (!defined($result)) {
  die $usage;
}

print "Operations complete.";
exit;

# End



# ------
# Backup
# ------

sub backup {

  #TODO: Not rely on env vars for connection params
  my $pgcheck = `psql -t -A -c "SELECT postgis_version()"` ||
    die "FATAL: Can't connect to database.  Please check connection parameters.\n";

  my @pgver = split(/ /,"$pgcheck");
  my $pgver = $pgver[0];


  # Check for PostGIS 1.x
  if (substr($pgver, 0, 1) != 1) {
    die "FATAL: PostGIS 1.x required for this operation.\n";
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
  for (my $count = 0; $count < $dbtot; $count++) {
    chomp($dblist[$count]);
    print "$dblist[$count] ";
  }
  print "}\n";

  # Dump each database to disk
  #TODO: Suppress ftell mismatch warning
  for my $db (@dblist) {
    print "Dumping: $db\n";
    my $dbdump = `pg_dump -Fc $db`;
    open (MYFILE, ">$dumppath/$db.dmp");
    print MYFILE $dbdump;
    close (MYFILE);
  }

  # Dump the database roles
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
  print "Users/Roles saved in:\n roles.sql\n";

}


# -------
# Restore
# -------

sub restore {

  # If on Windows and lacking postgis_restore.exe, try to muddle through
  # by hoping for Perl
  # TODO: Need to check for postgis_restore.exe when it's not on the path
  #       Below only checks for the current directory
  #  if (($os eq "MSWin32") && (! -f "postgis_restore.exe")) {
  #  print qq{
  #WARNING: postgis_restore.exe not found.  Will assume that you have Perl installed
  #         and try to move forward.\n
  #};
  #  $os = "test" # So that we can try running Perl below
  #}

  # Require postgis_restore.pl if not on Windows
  if ((! $os eq "MSWin32") && (! -f "postgis_restore.pl")) {
    die qq{
FATAL: postgis_restore.pl not found. Must be in current directory. This
       file can be found in your PostGIS 2 installation.\n
};
  }

  # Check that database is responding
  #TODO: Better way to do this?
  my $psqlcheck = `psql -t -A -d postgres -c "SELECT 1+1"` ||
    die "FATAL: Can't connect to database.  Please check connection parameters.\n";

  # Check for PostGIS 2.x
  #TODO: Not rely on env vars for connection params
  #TODO: May not be necessary to query postgres for postgis extension
  my $pgcheck = `psql -t -A -d postgres -c "SELECT default_version from pg_available_extensions where name = 'postgis'"` ||
    die "FATAL: PostGIS 2.x not found.\n";

  my @pgver = split(/ /,"$pgcheck");
  my $pgver = $pgver[0];
  chomp $pgver;

  # May not be necessary anymore
  if (substr($pgver, 0, 1) != 2) {
    die "FATAL: PostGIS 2.x required for this operation.\n";
  }
  print "PostGIS version $pgver found.\n";

  print "Restoring databases and roles from directory: $dumppath\n";

  # Restore the roles
  #TODO: What to do with role that already exist?
  print "Restoring roles...\n";
  my $dbrolerestore = `psql -f $dumppath/roles.sql` ||
    die "FATAL: roles.sql not found.  Maybe the dumppath isn't correct?";

  # Find all the dump files
  opendir my $dir, $dumppath || die "Cannot open directory: $!";
  my @dmpfiles = grep { -f && /\.dmp$/ } readdir $dir;
  closedir $dir;
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
    print "Creating new database in system...\n";
    #TODO: What if database already exists?
    my $createdb = `createdb $newdb`;
    print "Adding postgis extension to new database...\n";
    my $createpg = `psql -t -A -d $newdb -c "create extension postgis"`;
    my $newdbfile = $newdb.".dmp";
    print "Converting $newdbfile to PostGIS 2.0 format...\n";
    # Windows will run the .exe, others will run the separate Perl file
    my $convert;
    if ($os eq "MSWin32") {
      $convert = `postgis_restore.exe $newdbfile > $newdb.sql`
    } else { # All others will have Perl
      $convert = `perl postgis_restore.pl $newdbfile > $newdb.sql`;
    }
    unlink("$dumppath/$newdbfile.lst");
    # Did it work? If zero byte file, no
    my $filesize = -s "$newdb.sql";
    if ($filesize != 0) {
      print "File: $dumppath/$newdb.sql created.\n";
      print "Loading into PostGIS 2.0...\n";
      my $psql = `psql -d $newdb -f $newdb.sql`;
      print "Restore of database $newdb complete.\n\n";
    } else {
      print "WARNING: Conversion of $newdb database failed. Skipping...\n\n";
    } 
  }

}