# 1.ES详谈
# 2.ES索引的操作
# 3.ES数据的增删改
# 4.ES批量插入
# 5.ES数据的简单查询
# 6.ES数据的聚合查询

# 1.ES详谈
# 是什么，与其他搜索引擎比较，最好与华为的openlooken比一比，结合mysql谈谈自己的理解


# 2.ES索引的操作

# 2.1es客户端创建
from elasticsearch import Elasticsearch, helpers
from datetime import datetime
es = Elasticsearch([{"host": "127.0.0.1", "port": 9200}])
# 2.2es创建索引
# 2.2.1指定索引规则
# 注意：ES各个版本支持的映射语句结构上存在一定的差异，不同版本有不同版本的语句写法，参数有的版本支持，有的版本不支持
sets_and_maps = {
    "settings": {
        "index": {
            "number_of_shards": 5,
            "number_of_replicas": 1,
            "max_result_window": 100000000
        },
        "analysis": {
            "analyzer": {
                "default": {
                    "type": "ik_max_word"  # 设置分词器为最细粒度划分
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "employee": {
                "properties": {
                    "id": {
                        "type": "long"
                    },
                    "contract_period": {
                        "type": "date"
                    },
                    "age": {
                        "type": "integer"
                    },
                    "salary": {
                        "type": "long"
                    }
                }
            },
            "department": {
                "properties": {
                    "id": {
                        "type": "long"
                    },
                    "name": {
                        "type": "keyword"
                    },
                    "capacity": {
                        "type": "long"
                    }
                }
            }
        }
    }
}
# 插入数据格式
# data = {
#     "employee": {
#         "id": 123,
#         "contract_period": "2022-06-26",
#         "age": 25,
#         "salary": 5252525
#     },
#     "department": {
#         "id": 123,
#         "name": "2022-06-26",
#         "capacity": 25,
#     },
# }
# 2.2.2创建索引
# 注意：索引名必须为小写字母
#       不可重复创建
# result = es.indices.create(index="employees", body=sets_and_maps, ignore=400)
# print(result)
# 2.2.3es删除索引
# 会删除索引下所有数据
# result = es.indices.delete(index="employees")
# print(result)
# 2.2.4es获取索引
# result = es.indices.get(index="employees")
# print(result)
# 2.2.5es刷新索引
# ES的索引数据是写入到磁盘上的。但这个过程是分阶段实现的，因为IO的操作是比较费时的。
# 1.先写到内存中，此时不可搜索
# 2.默认经过 1s 之后会被写入 lucene 的底层文件 segment 中 ，此时可以搜索到
# 3.refresh 之后才会写入磁盘
# result = es.indices.refresh(index="employees")
# print(result)

# 3.ES数据的增删改
data = {
    "employee": {
        "id": 123,
        "contract_period": "2022-06-26",
        "age": 25,
        "salary": 5252525
    },
    "department": {
        "id": 123,
        "name": "2022-06-26",
        "capacity": 25,
    },
}
# 3.1插入数据

# 3.1.1create
# result = es.create(index='employees', id=1, body=data)
# print(result)
# # 3.1.2index
# result = es.index(index='employees', id=2, body=data)
# print(result)
# 3.1.3create与index对比
# create需指定id,index可指定，不指定会自动生成id
# create不能插入id相同的数据,index可以插入id相同的数据，此时可理解为删除原数据，再插入，若data字段与create插入时的字段一样，可理解为更新
# create方法内部其实也是调用index方法，是对index方法的封装
# 3.2删除数据
# result = es.delete(index='employees', id="2")
# print(result)
# 3.3更新数据

# 3.3.1update
# result = es.update(index='employees', id=1, body={'doc':data})
# print(result)
# # 3.3.2index
# result = es.index(index='employees', id=2, body=data)
# print(result)
# 3.1.3update与index对比
# update的body中可以只放需要更新的字段数据，若该字段并不存在于原body中，则为当前数据新增一个字段
# index可以理解为删除在插入，所以data中放的字段与元数据的字段一样时，才表现成更新

# 4.ES批量插入
# data = {
#     "employee": {
#         "id": 123,
#         "contract_period": "2022-06-26",
#         "age": 25,
#         "salary": 5252525
#     },
#     "department": {
#         "id": 123,
#         "name": "2022-06-26",
#         "capacity": 25,
#     },
# }
# 4.1 for
# start = datetime.now()
# for i in range(50000):
#     es.index(index='employees', id=i, body=data)
# end = datetime.now()
# print(end-start)  # 0:02:25.891614
# # 4.2 bulk
# start = datetime.now()
# actions = []
# for i in range(50000):
#     action = {'_op_type': 'index',
#               '_index': 'employees',
#               '_id': i,
#               '_source':  data
#     }
#     actions.append(action)
# helpers.bulk(client=es, actions=actions)
# end = datetime.now()
# print(end-start)  # 0:00:02.631896

# 4.3 streaming_bulk, 需要遍历才会运行，流式批量执行
# start = datetime.now()
# actions = []
# for i in range(5):
#     action = {'_op_type': 'index',
#               '_index': 'employees',
#               '_id': i,
#               '_source':  data
#     }
#     actions.append(action)
# for ok, info in helpers.streaming_bulk(es, actions):
#     print(ok, info)
# end = datetime.now()
# print(end-start)  # 0:00:02.374702

# 4.4 parallel_bulk, 需要遍历才会运行，并发批量执行，可以设置线程数
# start = datetime.now()
# actions = []
# for i in range(5):
#     action = {'_op_type': 'index',
#               '_index': 'employees',
#               '_id': i,
#               '_source':  data
#     }
#     actions.append(action)
# for ok, info in helpers.parallel_bulk(es, actions):
#     print(ok, info)
# end = datetime.now()
# print(end-start)  # 0:00:02.710983（4），0:00:01.712646（10）
# 结论：
# 1.for循环插入大量数据时速度非常慢，对于每条数据的创建都需要访问一次ES，放弃该方法
# 2.parallel_bulk() api是对bulk() api的包装，用于提供线程。 Parallel_bulk()返回一个生成器，必须使用该生成器生成结果。
# 3.如果对写入耗时要求不高，用bulk()即可；追求速度的话，用parallel_bulk()。
# 4.批量处理官方文档：https://elasticsearch-py.readthedocs.io/en/master/helpers.html#example

# 5.ES数据的查询

data = {
    "employee": {
        "id": 123,
        "contract_period": "2022-06-26",
        "age": 25,
        "salary": 5252525
    },
    "department": {
        "id": 123,
        "name": "2022-06-26",
        "capacity": 25,
    },
}

# 视频后缀名：.avi；.wmv；.mpg；.mpeg；.mov；.rm；.ram；.swf；.flv；.mp4
# 图像后缀名：.bmp；.jpg；.jpeg；.png；.gif；.pcd；.psd；mac；dwg；.cpx；.eps；；；；；；；；；；；；；；；；；；；；；
# 音频后缀名：.aiff；cd；wave；.mpg；.mpeg；.mp3；mpeg4；midi；wma；realaudio；vqf；oggvorbis；amr；ape；flac；aac；；
# 文本后缀名：.txt；.doc；.docx；.xls；.xlsx；.ppt；.pptx；.wps；.awd；.bak；.bat；.bin；.com；.c；.h；.cpp；.java；.py；.go；；；；；；；；；；；；；；；；；；；；
# 压缩包后缀名：.rar；.iso；.zip；.7z；.arj；.cab；.gz；.a；.z；.tar；.tgz；.bz2；.cgz；cpio；.rpm；.deb；；；；；；；；；；；；；；；；；；；；；；；；；；；；
# 其他后缀名：.exe；.html；.msi；.tmp；.mdf；.mid；.dbf；.dll；.lib；.fon；.get；.out；；
# https://wenku.baidu.com/view/8294b7c59a8fcc22bcd126fff705cc1755275fb4.html
