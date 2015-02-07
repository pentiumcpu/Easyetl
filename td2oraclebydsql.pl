#author date remark
#qichaopu 20150116 new create
my $tabmap="tab_map.conf";#�����ļ�tab_map.conf
my $column="column.conf";#�����ļ�tab_column.conf
#my $datapath="/data/cmmdm";#�����ļ�Ŀ¼
#my $ctlpath="/home/dwetl/etla/master/app/CMMDM/qcp/ctl";#�����ļ�Ŀ¼
#my $logpath="/home/dwetl/etla/master/app/CMMDM/qcp/log";#��־�ļ�
#my $tptpath="/home/dwetl/etla/master/app/CMMDM/qcp/tpt";#tpt�ļ�
#my $dsqlpath="/home/dwetl/etla/master/app/CMMDM/qcp/dsql";#dsql�ļ�
#my $sqlpath="/home/dwetl/etla/master/app/CMMDM/qcp/sql";#dsql�ļ�
#open(OUT,'+>',"/home/dwetl/etla/master/app/CMMDM/qcp/out.txt");
my $datapath="D:/pl/data";#�����ļ�Ŀ¼
my $ctlpath="D:/pl/ctl";#�����ļ�Ŀ¼
my $logpath="D:/pl/log";#��־�ļ�
my $tptpath="D:/pl/tpt";#tpt�ļ�
my $dsqlpath="D:/pl/dsql";#dsql�ļ�
my $sqlpath="D:/pl/sql";#sql�ļ�
open(OUT,'+>',"D:/pl/out.txt") || die "could not open out.txt file";;
open(TABMAP,'<',$tabmap) || die "could not open tab_map.conf file";
open(COLUMN,'<',$column) || die "could not open column.conf file";
my @lines=<COLUMN>;

main();
sub main
{
my @row;
my $rst="";
my $src;
my $dsc;
while(<TABMAP>)
        {
                chomp;
                @row=split("\t",$_);
                $src=$row[0];
                $dsc=$row[1];
                $rst=$rst.f_gen_sql($src,$dsc)."\n";
                #run_fexp_command(f_gen_sql($src,$dsc),$src);
                gen_ctl_file($src,$dsc);
        #        runsqlldr($src,$dsc);
                #unlink($datapath."/".$src.".dat");
                f_gen_exportfile($src,$dsc);
                #f_gen_file_dsql($src,$dsc);
                #f_gen_file_tpt($src,$dsc);
		#my $dsqlcmd="Dsql -c /home/dwetl/dsql/cm.env -f $dsqlpath/$dsc.dsql DB_NAME=CMM_VIEW_DEV1 TB_NAME=$dsc  TXDATE=20140930 FILEDIR=. DATAFILE=sjw.txt DATEFLD=20140930";
		#print $dsqlcmd."\n";
		#my $rc=system($dsqlcmd);
		#print $rc;
		#my $sedd="sed -i s/\$/,/g $datapath/$dsc.dat";
		#print $sedd;
		#system($sedd);
		#if($rc eq 0)
		#{
    #            runsqlldr($src,$dsc);
		#}
		#unlink("$datapath/$dsc.dat");
        }
        close(TABMAP);
        close(COLUMN);
        print (OUT $rst);
        close(OUT);
}
#����sql
sub f_gen_sql
{
        chomp;
        my($src,$des)=@_;
        my @col;
        my $sql="select ";
        my $line;
        #my $i=@lines;  #���鳤�ȣ�����
        #print $i."\n";  #��ӡ ������
        foreach $line (@lines)
        {
                chomp;
                @col=split("\t",$line);
                if($des eq $col[0])
                {
                        $sql=$sql.$col[1].",";
                }
        }
        $sql=substr($sql,0,length($sql)-1);
        $sql=$sql." from CMM_VIEW_DEV1.".$src;
        return $sql;
}

#����fexp����td�����ļ�
#$PAREXSQL  sql
#filename               �����ļ�����TD����
 sub run_fexp_command
{
 my($PAREXSQL,$filename)=@_;
 my $filepath=$datapath."/";
 print $filepath."\n";
 $logtable=$filename;
 $filename=$filename.".dat";
 print $filename."\n";
        if ( -d $datapath ) {
    #print "$temp_dir exists!\n";
                }
        else {
    mkdir $datapath;
        }
 $PAREXSQL=$PAREXSQL.";";
  print " $PAREXSQL"."\n";
 my $rc = open(BTEQ, "| fexp ");
 unless ($rc) 
 {
  print "Could not invoke BTEQ command/n";
  return -1;
 }
 my $logtable="TMP_DATA_DEV1.".$logtable;
 # ------ Below are BTEQ scripts ------ 
print BTEQ <<ENDOFINPUT;
.LOGON u_browser_dev1,pwd;
.LOGTABLE $logtable;
.BEGIN EXPORT ;
.EXPORT OUTFILE $filepath$filename MODE RECORD FORMAT TEXT;
 $PAREXSQL
   ;
.END EXPORT;
.LOGOFF;
.QUIT 0;                              
ENDOFINPUT
 close(BTEQ);
 my $RET_CODE = $? >> 8;
 if ( $RET_CODE == 12 ) {
  return 1;
 }
 else {
  return 0;
 }
}
##����sqlldr ctl�ļ��������ļ����ƺͱ�����һ��
#   $datafile  �����ļ�,��TDԴ����һ��
#   $ctlfile            �����ļ�����oracleĿ�����һ��
sub gen_ctl_file
{
        my($datafile,$ctlfile)=@_;
        my $cols="";
        my $line;
        foreach $line (@lines)
        {
                chomp;
                @col=split("\t",$line);
                if($ctlfile eq $col[0])
                {
			chomp($col[2]);
			chomp($col[3]);	
			if($col[3]=~/DATE/)
			{
				$cols=$cols.$col[2]." \"to_date(:$col[2],\'YYYY-MM-DD\')\",\n";
			}
			else
			{
                        	$cols=$cols.$col[2].",\n";
			}
                }
        }
        $cols=substr($cols,0,length($cols)-2);
        #$cols=substr($cols,0,length($cols)-15);
        my $outtext="load data\n ";
        my $outtext=$outtext."CHARACTERSET ZHS16GBK \n";
       # $outtext=$outtext."infile ";
       # $outtext=$outtext.$datapath."/".$datafile.".dat\n";
        $outtext=$outtext."TRUNCATE\n";
        $outtext=$outtext."INTO TABLE ".$ctlfile."\n";
        $outtext=$outtext."Fields terminated by \",\"\n";
        $outtext=$outtext."\n(\n".$cols."\n)\n";
        if ( -d $ctlpath ) {
                }
                else
                {
                        mkdir $ctlpath;
                }
        open(CTLFILE,'+>',$ctlpath."/".$ctlfile.".ctl");
        print CTLFILE $outtext;
        close(CTLFILE);
        #unlink($ctlpath."/".$ctlfile.".ctl");
}
##����sqlldr
sub runsqlldr
{
        my($datafile,$ctlfile)=@_;
                if ( -d $logpath ) {
                }
                else
                {
                        print "path: $logpath not exists,we will create the path:$logpath\n";
                        mkdir $logpath;
                }
        my $ldcmd="sqlldr cust_dm/cust_dm\@cmm control=$ctlpath/$ctlfile.ctl data=$datapath/$ctlfile.dat parallel=y log=$logpath/$ctlfile.log";
        print $ldcmd."\n";
        system($ldcmd);
}
#����dsql�ļ�
sub f_gen_file_dsql
{
	chomp(@_);
	my($src,$des)=@_;
	$dsqlfile=$des;
	my $outtext='/***************************************/
--����������DSQL TPT �ű�
--���ߣ��볯��
--���ڣ�2015-01-19
--��ڲ���: DB_NAME, TB_NAME, FILEDIR      , DATAFILE   ,DATEFLD      ,TXDATE
--          ж�ؿ� , ж�ر� , ж���ļ�Ŀ¼ , ж���ļ��� ,���������ֶ� ,ETL����,��ѯsql
--��������:Dsql -c $HOME/dsql/xchg_EXP.env -f xchg_exp0900.dsql DB_NAME=STG_DATA_DEV1 TB_NAME=CMS_TBL_COLL_CHRG_ACCT_BILL073 FILEDIR=. DATAFILE=sjw.txt TXDATE=20140930
/***************************************/
/*�����������Ƿ���ȫ*/
--SELECT TOP 1 1 FROM dbc.dbcinfo
--WHERE \'$DB_NAME\' =\'\' OR \'$TB_NAME\'=\'\' OR \'$FILEDIR\' =\'\' OR \'$DATAFILE\'=\'\' OR \'$TXDATE\'=\'\'
--;
--.IF ACTIVITYCOUNT>0 THEN .QUIT 1;
/*��ʼ������*/
SELECT \'^|\' SPLITCHAR
;
';
#$outtext=$outtext.'SET QUERY_BAND=\'EXPSCRIPT='.$dsqlfile.'.dsql;DB_NAME=$DB_NAME;TB_NAME=$TB_NAME;FILEDIR=$FILEDIR;DATAFILE=$DATAFILE;DATEFLD=$DATEFLD;TXDATE=$TXDATE\' FOR SESSION;';
$outtext=$outtext.'
..OS tbuild -f '."$tptpath/$dsqlfile.tpt -L $tptpath/tmp -j ".'${TB_NAME}${TXDATE} >'.$tptpath.'/tmp/${TB_NAME}${TXDATE}.tpt.log
..END';
								if ( -d $dsqlpath ) {
                }
                else
                {
                        mkdir $dsqlpath;
                }

	open(DSQLFILE,'+>',$dsqlpath."/".$dsqlfile.".dsql");
	print DSQLFILE $outtext;
}
#����tpt�ļ�
sub f_gen_file_tpt
{
	chomp(@_);
	my($src,$des)=@_;
	my @col;
	my $sql="select  ";	
	my $line;
	my $cols="";
	my $tptfile=$des; 
	foreach $line (@lines)
	{
		chomp;
		@col=split("\t",$line);
		if($des eq $col[0])
		{
			chomp($col[1]);
			chomp($col[3]);
			if($col[3]=~/DATE/)
			{
				$sql=$sql." CAST($col[1] AS VARCHAR(10)),\n"; 
				$cols=$cols.$col[1]." VARCHAR(10),\n";
			}
			else
			{
				$sql=$sql." CAST($col[1] AS $col[3]),\n";
				$cols=$cols.$col[1]." ".$col[3].",\n";
			}
		}
	}
	$sql=substr($sql,0,length($sql)-2);
	$sql=$sql;#."CAST(\'\' as VARCHAR(1))"; 
	$cols=substr($cols,0,length($cols)-2);
	#$cols=$cols;
	#$cols=$cols."temp VARCHAR(1)";
	$sql=$sql." from CMM_VIEW_DEV1.".$src.";";
	#ָ�����tpt�ļ�������
	my $outtext='DEFINE JOB export_public
DESCRIPTION \'export_public\'
(
    DEFINE SCHEMA SOURCE_SCHEMA
    (
       ';
      $outtext=$outtext.$cols."\n";
      $outtext=$outtext.');
    DEFINE OPERATOR Export_Operator
    TYPE EXPORT
    SCHEMA SOURCE_SCHEMA
    ATTRIBUTES
    (
        INTEGER MinSessions=1
        ,INTEGER MaxSessions=12
        ,VARCHAR TdpId=\'dbc\'
        ,VARCHAR UserName=\'t3_cmm_adm_dev1\'
        ,VARCHAR UserPassword=\'pwd\'
        ,VARCHAR SELECTSTMT=\'';
        $outtext=$outtext.$sql;
        $outtext=$outtext.'\'
        ,VARCHAR QueryBandSessInfo=\'USER=';
        $outtext=$outtext.$tptfile.".tpt";
        $outtext=$outtext.';\'
        ,VARCHAR CharacterSet=\'ASCII\'
        ,VARCHAR SpoolMode=\'noSpool\'
        ,VARCHAR DateForm=\'ANSIDATE\'
    );
    DEFINE OPERATOR FILE_WRITER
    TYPE DATACONNECTOR CONSUMER
    SCHEMA *
    ATTRIBUTES
    (
        VARCHAR DirectoryPath=\'';
     $outtext=$outtext.$datapath;
     $outtext=$outtext.'\'
        ,VARCHAR FileName=\'';
      $outtext=$outtext.$des.".dat";
      $outtext=$outtext.'\'
        ,VARCHAR Format=\'Delimited\'
        ,VARCHAR OpenMode=\'Write\'
        ,VARCHAR TextDelimiter=\',\'
    );
    APPLY TO OPERATOR(FILE_WRITER[1])
    SELECT * FROM OPERATOR(EXPORT_Operator[1]);
);'; 
								if ( -d $tptpath ) {
                }
                else
                {
                        mkdir $tptpath;
                } 
  
	open(TPTFILE,'+>',$tptpath."/".$tptfile.".tpt");
	print TPTFILE $outtext;
}
sub f_gen_exportfile
{
	chomp(@_);
	my($src,$des)=@_;
	my @col;
	my $sql="select  ";	
	my $line;
	my $cols="";
	my $sqlfile=$des; 
	foreach $line (@lines)
	{
		chomp;
		@col=split("\t",$line);
		if($des eq $col[0])
		{
			chomp($col[1]);
			chomp($col[3]);
			if($col[3]=~/DATE/)
			{
				$sql=$sql." TO_CHAR($col[1],\'YYYY-MM-DD\') $col[1],"; 
				$cols=$cols."rec.$col[1]||\',\'||";
			}
			if($col[3]=~/CLOB/)
			{
				$sql=$sql." TO_CHAR($col[1]) $col[1],"; 
				$cols=$cols."rec.$col[1]||\',\'||";
			}
			if(!($col[3]=~/CLOB/ || $col[3]=~/DATE/))
			{
				$sql=$sql." $col[1],";
				$cols=$cols."rec.$col[1]||\',\'||";
			}
		}
	}
	$sql=substr($sql,0,length($sql)-1);
	##$sql=$sql;#."CAST(\'\' as VARCHAR(1))"; 
	$cols=substr($cols,0,length($cols)-2);
	$sql=$sql." from ".$src;
        my $outtext='create or replace directory utlexportpath as \'/home/oracle/qcp/data\';
declare
    outfile utl_file.file_type;
begin';
        $outtext=$outtext."\n\toutfile := utl_file.fopen(\'UTLEXPORTPATH\',\'$des.dat\',\'W\');\n";
        $outtext=$outtext."for rec in ($sql)\n";
        $outtext=$outtext."loop\n";
        $outtext=$outtext."utl_file.put_line(outfile,$cols);\n";
        $outtext=$outtext.'end loop;
    utl_file.fclose(outfile);
end;
/';
        if ( -d $sqlpath ) {
                }
                else
                {
                        mkdir $sqlpath;
                }
        open(CTLFILE,'+>',$sqlpath."/".$sqlfile.".sql");
        print CTLFILE $outtext;
        close(CTLFILE);
}