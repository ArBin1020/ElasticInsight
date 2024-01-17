from hashlib import sha256
from elasticsearch import Elasticsearch
import json


answer = '9a6f8f6742f7403b55cdb9dd420f7448bbe88f1d3b30e60d898cd92b9301d89b'

def check(response):
    value = response.get('aggregations', {}).get('statistics', {}).get('value', {})
    j_str = json.dumps(value, sort_keys=True)
    hashed = sha256(j_str.encode('utf8')).hexdigest()
    status = True if hashed == answer else False
    return status

def get_gpsid_list():
    with open("gpsid_list.json", "r") as f:
        gpsid_list = json.load(f)
    return gpsid_list

query = {
    "match_all": {
    }
}

aggs = {
    "statistics": {
        "scripted_metric": {
            "params":{
                "gpsid_list" : get_gpsid_list()
            },
            "init_script": """
                state.transactions = new HashMap();
            """,
            "map_script": """
                String name = doc.name.value;
                String lv1 = name.substring(0, name.indexOf("|"));
                if(!state.transactions.containsKey(lv1)){
                    state.transactions[lv1] = 1;
                }
                else{
                    state.transactions[lv1] += 1;
                }
            """,
            "combine_script": """
                return state.transactions;
            """,
            "reduce_script": """
                Map result = states.remove(0);
                while(states.size() > 0){
                    Map data_buffer = states.remove(0);
                    for(lv1 in data_buffer.keySet()){
                        if(!result.containsKey(lv1)){
                            result[lv1] = data_buffer[lv1];
                        }
                        else{
                            result[lv1] += data_buffer[lv1];
                        }
                    }
                }
                return result;
            """
        }
    }
}

if __name__ == "__main__":
    es = Elasticsearch(hosts='localhost', port=9200)
    response = es.search(index="anal_product_2019-09", query=query, aggs=aggs, size=0)
    print(json.dumps(response, ensure_ascii=False, indent=4))
    print("Correct: ", check(response))