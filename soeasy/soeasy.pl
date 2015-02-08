###############################################################################
#author: 齐朝普
#function: 自动生成累全量sql
#modify history:
#--modifier    date        description
#--qichaopu   2015-01-22  初次建立
###############################################################################
use strict;
use warnings;
use Win32::OLE qw(in with);
use Win32::OLE::Const 'Microsoft Excel';
use Win32::OLE::NLS qw(:LOCALE :TIME);
my $Excel = Win32::OLE->GetActiveObject('Excel.Application')
        || Win32::OLE->new('Excel.Application', 'Quit');
$Excel->{DisplayAlerts}=0; 
my $path=`cd`; #获取当前目录
#$path=~s/\\/\\\\/g;
#print $path."\n";
chomp($path);
my $config="$path\\config.xls"; # $config excle配置表
my $sxjb="$path\\上线脚本v01.xlsx"; # $config excle配置表
my $result="$path\\result.xlsx"; # $result excle结果表
my $booksxjb=$Excel->Workbooks->Open($sxjb); #打开上线脚本
my $sxjball=$booksxjb->Worksheets("C1ALL");#上线脚本C1ALL页签
#上线脚本C1ALL页签最后一行
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
##累记全量
sub f_zeng2all
{
	my $bookrst= $Excel->Workbooks->Add();#新建工作簿
	my $bookconfig=$Excel->Workbooks->Open($config); #打开配置表
	#my $Sheetnew1 = $bookrst->Worksheets->Add({After=>$bookrst->WorkSheets($bookrst->WorkSheets->{Count})});
	my $Sheetavt1=$bookrst->Worksheets("Sheet1");
	$Sheetavt1->{Name}="result";
	my $pzb = $bookconfig->Worksheets("Sheet1");#配置表页签

	#$Sheet->Activate();
	#最后一行
	my $LastRow = $pzb->UsedRange->Find({What=>"*",
    SearchDirection=>xlPrevious,
    SearchOrder=>xlByRows})->{Row};
	print "最后一行:".$LastRow."\n";
	my $curs="";#当前源表
	my $curd="";#当前目标表
	my $isy="";#当前是否处理
	my $del="";
	my $insert="";
	foreach my $currow(2..$LastRow)  #$LastRow
	{
		$isy=$pzb->Cells($currow,4)->{Value};
		if(uc($isy) eq 'Y')
		{
		$curs=$pzb->Cells($currow,2)->{Value};
		$curd=$pzb->Cells($currow,3)->{Value};
		print "正在处理$curs 对应目标表为：$curd。。。。\n";
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
	$Sheetavt1->Cells(1,1)->{Value}="源表";
	$Sheetavt1->Cells(1,2)->{Value}="目标表";
	$Sheetavt1->Cells(1,3)->{Value}="DEL_SQL";
	$Sheetavt1->Cells(1,4)->{Value}="SQL_STR";
	#my $tmp=$pzb->Cells(1,1)->{Value};
	#print $tmp;
	#$Sheetavt1->Cells(1,1)->{Value}="测试";
	$bookrst->SaveAs($result);
	#$Excel->Workbooks->Close($config);
	#$Excel->Workbooks->Close($result);
}
sub f_gen_sql_del
{
		my ($src,$des)=@_;
		my $rst="DELETE FROM CUST_DM.$des A ";
		my $curd="";#当前目标表
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
	my $curd="";#当前目标表
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