python version 3.11.4

### docker 
  - Pull elasticsearch (7.16.2) images
    - ```docker pull elasticsearch:7.16.2```

  - Pull kibana (7.16.2) images
    - ```docker pull kibana:7.16.2```

  - Create network
    - ```docker network create elkwork```

  - Run elasticsearch images
    - ```docker run -d --name elasticsearch --net elkwork -p 9200:9200 -e “discovery.type=single-node” elasticsearch:7.16.2```

  - Run kibana images
    - ```docker run -d --name kibana --net elkwork -p 5601:5601 kibana:7.16.2```

### requirements: 
elasticsearch==7.16.2
### open [url](http://localhost:5601/app/dev_tools#/console)
### 建立分片
```
PUT /anal_product_2019-09
{
  "settings": {
    "index": {
      "number_of_shards": 5,  
      "number_of_replicas": 1 
    }
  }
}
```
### 定義索引
```
PUT /anal_product_2019-09/_mapping
{
  "properties": {
    "name":      {"type": "keyword", "index": true},
    "user_info": {"type": "keyword", "index": false}  
    }
  }
}
```
