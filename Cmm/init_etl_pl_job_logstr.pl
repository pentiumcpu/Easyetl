###############################################################################
#author: qichaopu
#function: ����ETL automation���õ�perl����
#�����ļ�: etl_job.ini
#modify history:
#--modifier    date        description
#--qichaopu   2015-02-04  �޸�
###############################################################################

use strict;
use File::Path qw( mkpath );
use File::Copy;

#--++Perl's package++--

#--++Public ETL perl++--
my $ETL_HOME = "C:/etltest" ;
#my $ETL_HOME="/home/dwetl/etla/master/app/CMMDM";
#my $ETL_HOME = $ENV{"AUTO_HOME"};
my $ETL_COMM_DIR = "$ETL_HOME/Common/Cmm" ;
my $ETL_APP_DIR = "$ETL_HOME/App" ;
#unshift(@INC, "$ETL_COMM_DIR");
#require( "etl_pub.pl" );

#--++��������++--
my $FILE_ETL_JOB_INI = "$ETL_COMM_DIR/etl_job.ini" ;
my $ret;

#--++ETL AUTOMATION CALL++--
print "--PID=$$\n";
main();
sub main
{
  #--++������ڲ���++--
  if ( $#ARGV < 0 ) {
    print "����ʧ�� ... ȱ�ٲ���\n";
    print "Usage: perl $0 <����ģʽ a-׷�� r-�ؽ�>\n";
    exit(1);
  }
  my $deal_type  =@ARGV[0];

  my $JOB_DIR = "$ETL_HOME/App" ;
  my $GROUP_NAME ;
  my $JOB_NAME ;
  my $JOB_NAME_TEMPLET ;

	my @line_list ;
	my $each_line ;
  my @para_set ;
	
  unless ( open(ETL_INI_FILE,"<","$FILE_ETL_JOB_INI") ) {
  	print "�����ļ��޷���: $FILE_ETL_JOB_INI\n";
  	return 1 ;
  }
  @line_list=<ETL_INI_FILE>;
  foreach $each_line(@line_list){
    chomp ( $each_line ) ;
    @para_set  = split('\t',$each_line) ;
  	$GROUP_NAME          = $para_set[0] ; 
   	$JOB_NAME            = $para_set[1] ; 
   	$JOB_NAME_TEMPLET    = $para_set[5] ; 

    #$JOB_NAME_TEMPLET = "$ETL_COMM_DIR/$JOB_NAME_TEMPLET" ;
    	
   	if ( ( $GROUP_NAME =~ /_JG_/ ) && ( $JOB_NAME =~ /_J_/ ) ) {
      #if ( ! -f $JOB_NAME_TEMPLET ) {
      #	print "ģ���ļ�������: $JOB_NAME_TEMPLET\n";
      #	return 1 ;
      #}
      #$JOB_DIR  = "$ETL_APP_DIR/" . $GROUP_NAME . "/" . substr($JOB_NAME,0,length($JOB_NAME) -3) ;
      $JOB_DIR  = "$ETL_APP_DIR" ;
      mkpath("$JOB_DIR",1,0755) ;
			gen_begin_file($JOB_NAME,$JOB_NAME_TEMPLET);
      $JOB_NAME = $JOB_DIR . "/" . $JOB_NAME ;
      if ( $deal_type eq "a" ) {
        if (! -f $JOB_NAME ) {
          #copy($JOB_NAME_TEMPLET,$JOB_NAME);
          print "$JOB_NAME ������\n" ;
        }
      }elsif ( $deal_type eq "r" ) {
          #copy($JOB_NAME_TEMPLET,$JOB_NAME);        
          print "$JOB_NAME �Ѹ���\n" ;
      }
    }

  }
  close ETL_INI_FILE;

  return $ret;
}


#exit($ret);
sub gen_begin_file
{
	
	my ($JOB_NAME,$f_begin_deal)=@_;
	$f_begin_deal=~s/cmm_ETL/f/g;	
	#$f_begin_deal=~s/\.pl//g;	
	$f_begin_deal=~s/templet/deal/g;
	$f_begin_deal=~s/deal_TD/deal_TD/g;
	print $f_begin_deal;
	open(PLFILE,'+>',$ETL_APP_DIR."/".$JOB_NAME);
	my $context='###############################################################################
#author: qichaopu
#function: AUTOMATION ����Oracle �洢����
#frequency:   D  (D-day,M-month,Q-quarter,Y-year)
#modify history:
#--modifier    date        description
#--xiejiangyong   2014-11-15  ���ν���
#--qichaopu   2015-02-04  �޸�
###############################################################################
use strict;

#--++Perl\'s package++--

#--++Public ETL perl++--
my $ETL_HOME="'.$ETL_HOME.'";
#   $ETL_HOME = $ENV{"AUTO_HOME"};
my $DIR_COMM = "$ETL_HOME/Common/Cmm" ;
my $DIR_DATA = "$ETL_HOME/DATA" ;
my $DIR_LOG = "$ETL_HOME/LOG" ;
my $LOGIN_HOST =$ENV{\'LOGIN_HOST\'};
my $LOGIN_PWD =$ENV{\'LOGIN_PWD\'};
my $LOGIN_UN =$ENV{\'LOGIN_UN\'};
unshift(@INC, "$DIR_COMM");
require( "etl_pub.pl" );

#--++��������++--
my $ret;

#--++ETL AUTOMATION CALL++--
print "--PID=$$\n";

sub main
{
  #--++������ڲ���++--
  if ( $#ARGV < 0 ) {
    print "����ʧ�� ... ȱ�����ڲ���\n";
    print "Usage: perl $0 <���ݴ�������>\n";
    exit(1);
  }
  my $TX_DATE  =@ARGV[0];

  my $ETL_DATE = f_conv_ETL_DATE ( $TX_DATE );
  my $DB_PARA = @ARGV[1];
  $DB_PARA="$LOGIN_HOST,$LOGIN_UN,$LOGIN_PWD,$LOGIN_HOST";
  my $DIR_PARA = "$DIR_COMM,$DIR_DATA,$DIR_LOG" ;

  $ret  = '.$f_begin_deal.' ( $ETL_DATE ,$DB_PARA ,$DIR_PARA );
  return $ret;
}
	$ret = main();
	exit($ret);';
	print PLFILE $context;
	close(PLFILE);
}