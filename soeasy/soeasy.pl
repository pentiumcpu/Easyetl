###############################################################################
#author: �볯��
#function: �Զ�������ȫ��sql
#modify history:
#--modifier    date        description
#--qichaopu   2015-01-22  ���ν���
###############################################################################
use strict;
use warnings;
use Win32::OLE qw(in with);
use Win32::OLE::Const 'Microsoft Excel';
use Win32::OLE::NLS qw(:LOCALE :TIME);
my $Excel = Win32::OLE->GetActiveObject('Excel.Application')
        || Win32::OLE->new('Excel.Application', 'Quit');
$Excel->{DisplayAlerts}=0; 
my $path=`cd`; #��ȡ��ǰĿ¼
#$path=~s/\\/\\\\/g;
#print $path."\n";
chomp($path);
my $config="$path\\config.xls"; # $config excle���ñ�
my $sxjb="$path\\���߽ű�v01.xlsx"; # $config excle���ñ�
my $result="$path\\result.xlsx"; # $result excle�����
my $booksxjb=$Excel->Workbooks->Open($sxjb); #�����߽ű�
my $sxjball=$booksxjb->Worksheets("C1ALL");#���߽ű�C1ALLҳǩ
#���߽ű�C1ALLҳǩ���һ��
my $lr = $sxjball->UsedRange->Find({What=>"*",
    SearchDirection=>xlPrevious,
    SearchOrder=>xlByRows})->{Row}+1;
print $config."\n";
print $result."\n";
print $sxjb."\n";
print $lr."\n";
main();
sub main
{
	f_zeng2all();
}
##�ۼ�ȫ��
sub f_zeng2all
{
	my $bookrst= $Excel->Workbooks->Add();#�½�������
	my $bookconfig=$Excel->Workbooks->Open($config); #�����ñ�
	#my $Sheetnew1 = $bookrst->Worksheets->Add({After=>$bookrst->WorkSheets($bookrst->WorkSheets->{Count})});
	my $Sheetavt1=$bookrst->Worksheets("Sheet1");
	$Sheetavt1->{Name}="result";
	my $pzb = $bookconfig->Worksheets("Sheet1");#���ñ�ҳǩ

	#$Sheet->Activate();
	#���һ��
	my $LastRow = $pzb->UsedRange->Find({What=>"*",
    SearchDirection=>xlPrevious,
    SearchOrder=>xlByRows})->{Row};
	print "���һ��:".$LastRow."\n";
	my $curs="";#��ǰԴ��
	my $curd="";#��ǰĿ���
	my $isy="";#��ǰ�Ƿ���
	my $del="";
	my $insert="";
	foreach my $currow(2..$LastRow)  #$LastRow
	{
		$isy=$pzb->Cells($currow,4)->{Value};
		if(uc($isy) eq 'Y')
		{
		$curs=$pzb->Cells($currow,2)->{Value};
		$curd=$pzb->Cells($currow,3)->{Value};
		print "���ڴ���$curs ��ӦĿ���Ϊ��$curd��������\n";
		$del=f_gen_sql_del($curs,$curd);
		$insert=f_gen_sql_insert($curs,$curd);
		#print "$del\n";
		#print "$insert\n";
		$Sheetavt1->Cells($currow,1)->{Value}="CUST_DM.$curs";
		$Sheetavt1->Cells($currow,2)->{Value}="CUST_DM.$curd";
		$Sheetavt1->Cells($currow,3)->{Value}=$del;
		$Sheetavt1->Cells($currow,4)->{Value}=$insert;
	}
	}
	$Sheetavt1->Cells(1,1)->{Value}="Դ��";
	$Sheetavt1->Cells(1,2)->{Value}="Ŀ���";
	$Sheetavt1->Cells(1,3)->{Value}="DEL_SQL";
	$Sheetavt1->Cells(1,4)->{Value}="SQL_STR";
	#my $tmp=$pzb->Cells(1,1)->{Value};
	#print $tmp;
	#$Sheetavt1->Cells(1,1)->{Value}="����";
	$bookrst->SaveAs($result);
	#$Excel->Workbooks->Close($config);
	#$Excel->Workbooks->Close($result);
}
sub f_gen_sql_del
{
		my ($src,$des)=@_;
		my $rst="DELETE FROM CUST_DM.$des A ";
		my $curd="";#��ǰĿ���
		my $curd1=$sxjball->Cells(3,3)->{Value};
		#print $curd1;
		my $ispk;
		my $col="";
		my $where="WHERE EXISTS (SELECT 1 FROM CUST_DM.$src B WHERE ";
		for (my $cr=4;$cr<=$lr;$cr++)
		{
			$curd=$curd1;
			$curd1=$sxjball->Cells($cr,3)->{Value};
			$col=$sxjball->Cells($cr-1,5)->{Value};
			$ispk='';
			$ispk=$sxjball->Cells($cr-1,7)->{Value};
			if(($curd eq $des) && ($ispk eq 'Y'))
			{
				$where="$where A.$col=B.$col AND ";
			}
			if(($curd eq $des) && ($curd ne $curd1))
			{
				$where=substr($where,0,length($where)-4);
				#print $where;
				$where=$where.")";
				last;
			}
		}
		$rst="$rst $where";
		return "$rst";
}
sub f_gen_sql_insert
{
	my ($src,$des)=@_;
	my $rst="";
	my $insert="";
	my $sel="SELECT ";
	my $curd="";#��ǰĿ���
	my $curd1=$sxjball->Cells(3,3)->{Value};
	my $col="";
	$insert="INSERT INTO CUST_DM.$des (\n";
	for (my $cr=4;$cr<=$lr;$cr++)
		{
			$curd=$curd1;
			$curd1=$sxjball->Cells($cr,3)->{Value};
			$col=$sxjball->Cells($cr-1,5)->{Value};
			if(($curd eq $des) && ($curd eq $curd1))
			{
				$insert="$insert $col,\n";
				$sel="$sel $col,\n";
			}
			if(($curd eq $des) && ($curd ne $curd1))
			{
				$insert="$insert $col)\n";
				$sel="$sel :ETL_DATE_D\n";
				$sel="$sel FROM CUST_DM.$src";
				last;
			}
		}
	$rst="$insert $sel";
	return "$rst";
}