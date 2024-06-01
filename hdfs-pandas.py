import hdfs
import pandas as pd

# hdfs的主机地址和用户名以及csv文件路径配置
hdfs_url = "http://172.18.121.19:9870"
hdfs_user = "root"
file_path = "/put/domains.csv"
# 连接hdfs集群
client = hdfs.InsecureClient(hdfs_url,hdfs_user)
# 读取csv文件
with client.read(hdfs_path=file_path,encoding="utf-8") as f:
    df = pd.read_csv(f)
# 生成新列，提取url中的域名

df['域名'] = df['url'].str.extract(r'http[s]?://([^/]+)')
# 生成新dataframe，统计域名出现的次数
df['次数'] = df.groupby('域名')['域名'].transform('count').sort_values(ascending=False)
df2 = pd.DataFrame(df[['域名','次数']])
# 将结果写入hdfs集群(先将旧文件删除)
client.delete(hdfs_path='/result/mr_alldata.csv')
with client.write(hdfs_path='/result/mr_alldata.csv',encoding="utf-8") as g:
    df2.to_csv(g,index=False)
client.close()
print(df2,"\n写入成功")
