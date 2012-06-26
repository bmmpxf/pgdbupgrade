# OpenGeo Suite PostGIS database upgrade script
# Automates the data loading process from OpenGeo Suite 2.5 to 3.0
# (PostGIS 1.5 to PostGIS 2.0)


#TODO: Usage!
#TODO: Error handling!

# Connect to Postgres
use DBI;
package DBD::pg;

# Define vars
my $dumppath = ".";
#TODO: Ask for $dumppath as an argument
#TODO: Check for createdb, psql, pg_dump, pg_dumpall, postgis_restore.pl


# -------------------------
# DBI Graveyard
#TODO: None of this has worked yet

#my $dbh = DBI->connect("dbi:Pg:dbname=medford;host=localhost;port=54321",'postgres','',{AutoCommit => 0, RaiseError => 1, PrintError => 0});

#print "3+3=",$dbh->selectrow_array("SELECT 3+3"),"\n";

#@dblist = $dbh->data_sources("port=54321");


#my @dblist = $dbh->execute("SELECT datname FROM pg_database WHERE datistemplate IS FALSE and datname NOT LIKE 'postgres'");
#@dblist = $dbh->data_sources();

# -------------------------



# Get a list of all relevant databases
#TODO: How to exclude non-spatial DBs?
my @dblist = `psql -t -A -p 54321 -d postgres --command "SELECT datname FROM pg_database WHERE datistemplate IS FALSE and datname NOT LIKE 'postgres';"`;

# Total number of databases found
my $dbtot = scalar @dblist;

print "Found the following $dbtot databases:\n";
for ($count = 0; $count < $dbtot; $count++) {
  chomp($dblist[$count]);
  print "$dblist[$count]\n";
}


# dump each database to disk
#TODO: Suppress warning:
# pg_dump: [custom archiver] WARNING: ftell mismatch with expected position -- ftell used
for my $db (@dblist) {
  print "Dumping: $db\n";
  my $dbdump = `pg_dump -p 54321 -Fc $db`;
  open (MYFILE, ">$dumppath/$db.dmp");
  print MYFILE $dbdump;
  close (MYFILE);
}

# dump the database roles
print "Dumping: roles\n";
my $dbroledump = `pg_dumpall -p 54321 -r`;
open (MYFILE, ">$dumppath/roles.sql");
print MYFILE $dbroledump;
close (MYFILE);


# This is now the restore operation.
#TODO: Separate this with command arguments

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
  my $newdbfile = $newdb,".dmp";
  print "Coverting $newdbfile to PostGIS 2.0 format:\n";
  my $convert = `postgis_restore.pl $newdbfile`;
  print "Loading into PostGIS 2.0:\n";
  my $psql = `psql $newdb $newdbfile`;
  print "Restore of database $newdb complete\n\n";
}

#UNKNOWN: Do we need to "create extension postgis" on this new db? 

print "Operation complete\n";

