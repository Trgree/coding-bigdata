
程序功能：

使用MR分地市读取用户标签数据dmp.dm_userlabel_http，写入到redis

1.dm_userlabel_http格式：
userId|labelId

2.输出到redis 
数据类型:SET
sLabelAreaUser
{
	string username; /*标签用户信息宽带账号+UA*/
}
key命名方式为sLabelAreaUser:labelid:403000，labelid为标签id，
如sLabelAreaUser:101:440300,也即标签101下所有标签用户信息

