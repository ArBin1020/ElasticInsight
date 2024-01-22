import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

class aggs{
    public static void main(String[] args){
        Set gpsid_list = new HashSet(params.gpsid_list);
        Map result = new HashMap(); // 存放分析結果
        Map visitedMap = new HashMap(); // 存放已經分析過的 lv1

        while (states.size() > 0) {
            Map data = states.remove(0); // data = {'個人清潔':{}}
            for (lv1 in data.keySet()) {
                int personal_count = 0, times_count = 0; // 受眾人數, 受眾次數
                int total_personal_count = 0, total_times_count = 0;// 所有人數, 所有次數


                // 人數如果沒有重複的話，就是 data[lv1].get(id)                     
                for(id in data.keySet()) {
                    Set visited = visitedMap.computeIfAbsent(lv1, k -> new HashSet());
                    if (gpsid_list.contains(id)) {
                        times_count += !visited.contains(id) ? data[lv1].get(id) : 0;
                        personal_count += 1;
                    }
                    total_personal_count += 1;
                    total_times_count += !visited.contains(id) ? data[lv1].get(id) : 0;
                }
                visited.add(id);

                if (!result.containsKey(lv1)) {
                    result[lv1].put("受眾人數", personal_count);
                    result[lv1].put("受眾次數", times_count);
                    result[lv1].put("所有人數", total_personal_count);  
                    result[lv1].put("所有次數", total_times_count);
                }
                else {
                    result[lv1].put("受眾人數", result[lv1].get("受眾人數") + personal_count);
                    result[lv1].put("受眾次數", result[lv1].get("受眾次數") + times_count);
                    result[lv1].put("所有人數", result[lv1].get("所有人數") + total_personal_count);
                    result[lv1].put("所有次數", result[lv1].get("所有次數") + total_times_count);
                }
            }
        }
        return result;
    }
}