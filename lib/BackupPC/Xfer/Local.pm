#============================================================= -*-perl-*-
#
# BackupPC::Xfer::Local package
#
# DESCRIPTION
#
#   This library defines a BackupPC::Xfer::Local class for performing
#   backups of locally-accessible files.
#
# AUTHOR
#   Paul Mantz  <pcmantz@zmanda.com>
#
# COPYRIGHT
#   Copyright (C) 2009 Zmanda, Inc.
#
#   This program is free software; you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation; either version 2 of the License, or
#   (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program; if not, write to the Free Software
#   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
#
#========================================================================
#
# Version __VERSION__, released __DATE__.
#
# See http://backuppc.sourceforge.net.
#
#========================================================================

package BackupPC::Xfer::Local;

use Fcntl qw/:mode/;
use File::stat qw/:FIELDS/;
use File::Find;
use File::Path;
use POSIX qw/mkfifo/;

use BackupPC::View;
use Encode qw/from_to encode/;
use base qw( BackupPC::Xfer::Protocol );

use vars qw( $UnixMknodOK );

BEGIN {

    undef $@;
    eval "use Unix::Mknod;";
    $UnixMknodOK = !( defined $@ );
};


sub start
{
    my ($t)      = @_;
    my $bpc      = $t->{bpc};
    my $conf     = $t->{conf};
    my $logMsg   = undef;
    my $incrDate = $t->{incrDate};

    ( $t->{shareNameSlash} = "$t->{shareName}/" ) =~ s{//+$}{/};

    from_to( $t->{shareName}, "utf8", $conf->{ClientCharset} )
      if ( $conf->{ClientCharset} ne "" );
    from_to( $t->{shareNameSlash}, "utf8", $conf->{ClientCharset} )
      if ( $conf->{ClientCharset} ne "" );

    if ( $t->{type} eq "restore" ) {
        $logMsg = "restore started for directory $t->{shareName}";

    } else {
        $bpc->backupFileConfFix( $conf, "LocalShareName" );

        if ( $t->{type} eq "full" ) {
            $logMsg = "full backup started for directory $t->{shareName}";
        } else {
            $incrDate = $bpc->timeStamp( $t->{incrBaseTime} - 3600, 1 );
            $logMsg = "incr backup started back to $incrDate"
              . " (backup #$t->{incrBaseBkupNum}) for directory"
              . " $t->{shareName}";
        }
    }

    #
    # Clean up and return
    #
    delete $t->{_errStr};
    return $logMsg;
}


sub run
{
    my ($t)  = @_;
    my $bpc  = $t->{bpc};
    my $conf = $t->{conf};

    if ( $t->{type} eq "restore" ) {
        $t->restore();
    } else {
        $t->backup();
    }

    if ( $t->{type} eq "restore" ) {
        my @results =
          ( $t->{fileCnt}, $t->{byteCnt}, $t->{errCnt}, $t->{_errStr}, );

    } else {
        #
        # TODO: make statistics more meaningful
        #
        my @results = (
            0,                # $tarErrs
            $t->{fileCnt},    # $nFilesExist
            $t->{byteCnt},    # $sizeExist
            0,                # $sizeExistComp
            $t->{fileCnt},    # $nfilesTotal
            $t->{byteCnt},    # $sizeTotal
        );
    }

    $t->{xferOK} = !( $t->{_errStr} );
    return wantarray ? @results : \@results;
}


#
# Backup Routines
#
sub backup
{
    my ($t)    = @_;
    my $bpc    = $t->{bpc};
    my $conf   = $t->{conf};
    my $TopDir = $bpc->TopDir();
    my $OutDir = $t->{OutDir} = "$TopDir/pc/$t->{client}/new/"
      . $bpc->fileNameEltMangle( $t->{shareName} );
    my $Attrib = $t->{Attrib} = {};

    $t->{view} = BackupPC::View->new( $bpc, $t->{client}, $t->{backups} );

    #
    # use File::Find iteration over the target shares.
    #
    $t->{wanted} =
      sub { $t->wanted( $_, $File::Find::dir, $File::Find::name ); };
    find(
        {
            wanted   => $t->{wanted},
            no_chdir => 0,
        },
        $t->{shareName}
    );

    #
    # flush any remaining attrib files
    #
    while ( my ( $dir, $attrib ) = each %$Attrib ) {
        $t->attributeWrite( $dir, $attrib );
    }
}


sub wanted
{
    my ( $t, $name, $dir, $fullName ) = @_;

    my $bpc        = $t->{bpc};
    my $OutDir     = $t->{OutDir};
    my $Attrib     = $t->{Attrib};
    my $shareName  = $t->{shareName};
    my $newFilesFH = $t->{newFilesFH};

    my $data = undef;
    my $fh   = undef;

    unless ( $t->checkIncludeExclude($fullName) ) {
        $File::Find::prune = 1;
        return;
    }

    $fullName =~ /$shareName(.*)/;
    my $relName = ( $1 eq "" ) ? "/" : $1;
    $relName =~ /(.*)\/$name/;
    my $relDir = ( $1 eq "" ) ? "/" : $1;

    #
    # using File::stat makes stat() return an object instead of a
    # tuple.
    #
    my $st         = stat($fullName);
    my $attribInfo = {
        type  => $st_type,
        mode  => $st_mode & 0777,
        uid   => $st_uid,
        gid   => $st_gid,
        size  => $st_size,
        mtime => $st_mtime,
    };

    if ( !defined $Attrib->{$relDir} ) {
        foreach my $d ( keys %$Attrib ) {
            next if $relDir =~ /^$d/;
            $t->attributeWrite( $d, $Attrib->{$d} );
        }
        $Attrib->{$relDir} =
          BackupPC::Attrib->new( { compress => $t->{compress} } );
    }

    if ( S_ISDIR( $st->mode ) ) {

        #
        # Process the directory and return.
        #
        $attribInfo->{type} = BPC_FTYPE_DIR;
        eval { mkpath( "$OutDir/" . $bpc->fileNameMangle($relName), 0, 0755 ); };
        $t->logFileAction( "create", $name, $st );
        $Attrib->{$relDir}->set( $name, $attribInfo );
        return;

    } elsif ( S_ISREG( $st->mode ) ) {
        $attribInfo->{type} = BPC_FTYPE_FILE;
        open $fh, "<", $fullName;

    } elsif ( S_ISLNK( $st->mode ) ) {
        $attribInfo->{type} = BPC_FTYPE_SYMLINK;
        $data = readlink $fullName;

    } elsif ( S_ISBLK( $st->mode ) ) {
        $attribInfo->{type} = BPC_FTYPE_BLOCKDEV;

        if ($UnixMknodOK) {
            my $devmajor = major($st_rdev);
            my $devminor = minor($st_rdev);
        } else {
            my $devmajor = ( $st_dev & 0xff00 ) >> 8;
            my $devminor = ( $st_dev & 0xff ) | ( $st_dev >> 12 & ~0xff );
        }
        $data = "$devmajor,$devminor"

    } elsif ( S_ISCHR( $st->mode ) ) {
        $attribInfo->{type} = BPC_FTYPE_CHARDEV;

        if ($UnixMknodOK) {
            my $devmajor = major($st_rdev);
            my $devminor = minor($st_rdev);
        } else {
            my $devmajor = ( $st_dev & 0xff00 ) >> 8;
            my $devminor = ( $st_dev & 0xff ) | ( $st_dev >> 12 & ~0xff );
        }
        $data = "$devmajor,$devminor";

    } elsif ( S_ISFIFO( $st->mode ) ) {
        $attribInfo->{type} = BPC_FTYPE_FIFO;
        $data = "";

    } elsif ( S_ISSOCK( $st->mode ) ) {
        $File::Find::prune = 1;
        return;

    } else {
        $File::Find::prune = 1;
        return;
    }

    unless ( defined $fh xor defined $data ) {
        $t->logFileAction( "fail", $fullName, $attribInfo );
    }

    #
    # Write the file
    #
    my $poolFile  = $OutDir . "/" . $bpc->fileNameMangle($relName);
    my $poolWrite = BackupPC::PoolWrite->new(
        $bpc, $poolFile,
        ( defined $fh ) ? $st->size : length $data,
        $t->{compress} );

    my $datasize;
    if ( defined $fh ) {
        $dataSize = 0;
        while (<$fh>) {
            $dataSize += length($_);
            $poolWrite->write( \$_ );
        }

    } elsif ( defined $data ) {
        $dataSize = length $data;
        $poolWrite->write( \$data );
    }

    my ( $exists, $digest, $outSize, $errs ) = $poolWrite->close();
    if (@$errs) {
        $t->logFileAction( "fail", $fullName, $attribInfo );
        unlink($poolFile);
        $t->{xferBadFileCnt}++;
        $t->{errCnt} += scalar(@$errs);
        return;
    }

    $t->logFileAction( $exists ? "pool" : "create", $fullName, $attribInfo );
    print $newFilesFH "$digest $st->size $poolFile\n" unless $exists;
    $t->{byteCnt} += $dataSize;
    $t->{fileCnt}++;

    #
    # finish up
    #
    $Attrib->{$relDir}->set( $name, $attribInfo );
}


#
# helper functions for the wantedFiles subroutine
#
sub attributeWrite
{
    my ( $t, $dir, $attrib ) = @_;

    my $bpc      = $t->{bpc};
    my $Attrib   = $t->{Attrib};
    my $OutDir   = $t->{OutDir};

    if ( $Attrib->{$dir}->fileCount ) {
        my $data     = $attrib->writeData;
        my $fileName = $attrib->fileName("$OutDir/$dir");
        my $poolWrite =
            BackupPC::PoolWrite->new( $bpc, $fileName, length $data, $t->{compress} );
        $poolWrite->write( \$data );
    }
}


#
# Restore Routines
#
sub restore
{
    my ($t)      = @_;
    my $conf     = $t->{conf};
    my $fileList = $t->{fileList};

    my $view = $t->{view} =
      BackupPC::View->new( $t->{bpc}, $t->{bkupSrcHost}, $t->{backups} );
    $t->{hardLinkPoolFiles} = {};

    $t->{shareDest}   =~ s{/*$}{};
    $t->{pathHdrDest} =~ s{^/*}{};

    if ( $t->{shareDest} !~ m/^\// ) {
        $t->{_errStr} =
          "\$t->{shareDest} must be an absolute path (given $t->{shareDest})";
        return;

    } elsif ( !( -d $t->{shareDest} ) ) {
        $t->{_errStr} =
          "\$t->{shareDest} must be an existing path (given $t->{shareDest})";
        return;

    } elsif ( !( -d "$t->{shareDest}/$t->{pathHdrDest}" ) ) {

        undef $@;
        eval {
            mkpath( "$t->{shareDest}/$t->{pathHdrDest}",
                1, 0777 & $conf->{UmaskMode} );
        };
        if ($@) {
            $t->{_errStr} = "can't create restore directory "
              . "$t->{shareDest}/$t->{pathHdrDest}";
            return;
        }
    }

    my $i = 0;
    do {
        $i++;
        my $testFile = "$t->{shareDest}}/$t->{pathHdrDest}/testFile.$$.$i";
    } while ( -e $testFile );

    my $fd;
    if ( !open $fd, ">", $testFile ) {
        $t->{_errStr} = "can't write to restore directory "
          . "$t->{shareDest}/$t->{pathHdrDest}";
    }
    close $fd;

    #
    # restore each directory
    #
    map { $t->restoreDirEnt($_); } @$fileList;
    $t->restoreWriteHardLinks();
}


sub restoreDirEnt
{
    my ( $t, $dirEnt ) = @_;
    my $dst = $t->{shareDest} . "/" . $t->{pathHdrDest} . "/" . $dirEnt;
    my $fileAttrib = $t->{view}->fileAttrib($dirEnt);

    if ( $fileAttrib->{type} eq BPC_FTYPE_DIR ) {
        $t->restoreDir($dirEnt);

    } elsif ( $fileAttrib->{type} eq BPC_FTYPE_FILE ) {
        $t->restoreFile($dirEnt);

    } elsif ( $fileAttrib->{type} eq BPC_FTYPE_HARDLINK ) {
        $t->restoreHardLink($dirEnt);

    } elsif ( $fileAttrib->{type} eq BPC_FTYPE_SYMLINK ) {
        $t->restoreSymlink($dirEnt);

    } elsif ( $fileAttrib->{type} == BPC_FTYPE_CHARDEV
           || $fileAttrib->{type} == BPC_FTYPE_BLOCKDEV
           || $fileAttrib->{type} == BPC_FTYPE_FIFO ) {
        $t->restoreDevice($dirEnt);

    } else {
        $t->logFileAction( "fail", $name, $fileAttrib );
    }
}


sub restoreDir
{
    #
    # UNTESTED: factored out to handle recursion better
    #
    my ( $t, $dir ) = @_;
    my $conf = $t->{conf};
    my $view = $t->{view};

    my $dst = $t->{shareDest} . "/" . $t->{pathHdrDest} . "/" . $dir;
    $dst =~ s{//+}{/}g;

    my $dirAttrib = $view->fileAttrib($dir);
    my @fileList  = @{ $view->dirAttrib($dir) };

    eval { mkpath( $dst, 1, $dirAttrib->{mode} & $conf->{UmaskMode} ); };
    if ($@) {
        $t->logFileAction( "fail", $dir, $dirAttrib );
    }

    map { $t->restoreDirEnt($_) } @fileList;
}



sub restoreFile
{
    #
    # UNTESTED: Factored out to reduce code duplicity
    #
    my ( $t, $file ) = @_;
    my $fileAttrib = $t->{view}->fileAttrib($file);

    my $dst = $t->{shareDest} . "/" . $t->{pathHdrDest} . "/" . $file;
    my ($dstfh);

    my $f = BackupPC::FileZIO->open( $fileAttrib->{fullPath},
        0, $fileAttrib->{compress} );
    open $dstfh, ">+", $dst;

    if ( !defined $dstfh || !defined $f ) {
        $t->logFileAction( "fail", $name, $fileAttrib );
    }

    while ( $f->read( \$data, $BufSize ) > 0 ) {
        print $dstfh, $data;
    }
    $t->{fileCnt}++;
}


sub restoreHardLink
{
    #
    # INCOMPLETE: Write this data only if it hasn't been written yet;
    # i.e., cache all the written pool files, and if the linked file
    # hasn't been written yet, write it.  otherwise, hardlink it.
    #
    my ( $t, $link ) = @_;
    my $conf          = $t->{conf};
    my $poolHardlinks = $t->{hardLinkPoolFiles};

    my $linkAttrib = $t->{view}->fileAttrib($link);
    my $poolFile   = $linkAttrib->{fullPath};

    if ( defined $poolHardLinks->{$poolFile} ) {
        push @{ $poolHardLinks->{$poolFile} }, $link;
    } else {
        $poolHardLinks->{$poolFile} = [$link];
    }
    $t->{fileCnt}++;
}


sub restoreWriteHardLinks
{
    #
    # INCOMPLETE: write the first file to disk normally, then link the
    # others to it.
    #
    my ($t) = @_;

    while ( my ( $poolFile, $fileList ) = each %{ $t->{hardLinkPoolFiles} } ) {

        next if ( @$fileList == 0 );

        my $firstFile = shift @$fileList;
        $t->restoreWriteFile($firstFile);
        map { link $firstFile, $_ } @$fileList;
    }
}


sub restoreSymlink
{
    #
    # UNTESTED: factored out to reduce duplicity and enhance readability
    #
    my ( $t, $link ) = @_;

    my $linkAttrib = $t->{view}->fileAttrib($link);

    my $dst = $t->{shareDest} . "/" . $t->{pathHdrDest} . "/" . $link;
    my $data;

    my $l = BackupPC::FileZIO->open( $linkAttrib->{fullPath},
        0, $linkAttrib->{compress} );

    if ( !defined $l || $f->read( \$data, $BufSize ) < 0 ) {
        $t->logFileAction( "fail", $link, $linkAttrib );
        $l->close if ( defined $l );
        $t->{errCnt}++;
        return;
    }

    my $symTarget = "";
    while ( $l->read( \$data, $BufSize ) > 0 ) {
        $symTarget .= $data;
    }

    symlink $symTarget, $dst;
    $t->{fileCnt}++;
}


sub restoreDevice
{
    my ( $t, $dev ) = @_;

    my $devAttrib = $t->{view}->fileAttrib($dev);

    my $dst = $t->{shareDest} . "/" . $t->{pathHdrDest} . "/" . $dev;
    my ($dstfh);

    my $d = BackupPC::FileZIO->open( $devAttrib->{fullPath},
        0, $devAttrib->{compress} );
    my $data;

    if ( !defined($d) || $f->read( \$data, $BufSize ) < 0 ) {
        $t->logFileAction( "fail", $dev, $devAttrib );
        $d->close if ( defined $d );
        $t->{errCnt}++;
        return;
    }
    $d->close;
    if ( $data =~ /(\d+),(\d+)/ ) {
        my $devmajor = $1;
        my $devminor = $2;
    }
    $t->restoreWriteDevice( $dst, $devAttrib->{mode}, $devmajor, $devminor )
      or $t->logFileAction( "fail", $dev, $devAttrib );
}


sub restoreWriteDevice
{
    #
    # UNTESTED: skullduggery with system calls and Unix::Mknod
    #
    my $t    = shift @_;
    my $dst  = shift @_;
    my $mode = shift @_;
    my $maj  = shift @_;
    my $min  = shift @_;

    if ($UnixMknodOK) {
        mknod( $dst, S_IFCHR | $Conf{UmaskMode}, makedev( $maj, $min ) );

    } else {
        #
        # use system calls
        #
        if ( defined $maj && defined $min ) {
            system split /\s/, "/bin/mknod $dst $maj $min";
        } else {
            system split /\s/, "/usr/bin/mkfifo $dst";
        }
        system( "/bin/chmod", ( $mode | $Conf{UmaskMode} ), $dst );
    }
}


sub logFileAction
{
    my ( $t, $action, $name, $st ) = @_;

    #
    # using the $st_* variables is ugly, but it works
    #
    my $owner      = "$st_uid/$st_gid";
    my $fileAction = sprintf( "  %-6s %1s%4o %9s %11.0f %s\n",
                              $action, $st_type, $st_mode & 07777,
                              $owner,  $st_size, $name );
    return $t->logWrite( $fileAction, 1 );
}


1;
