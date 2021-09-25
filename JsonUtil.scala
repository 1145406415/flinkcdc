package hbh.utils

import java.util

import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser

import scala.collection.mutable

object JsonUtil {

    def jsonToMap(json_str: String): util.HashMap[String, String] = {
        val map: util.HashMap[String, String] = new util.HashMap[String, String]()
        if (json_str != "" && json_str != null) {
            val parserJson: JSONParser = new JSONParser()
            try {
                parserJson.parse(json_str) match {
                    case jsonObj: JSONObject =>
                        val jsonKey: util.Set[String] = jsonObj.keySet()
                        val iter: util.Iterator[String] = jsonKey.iterator()
                        while (iter.hasNext) {
                            val instance: String = iter.next()
                            if (!(jsonObj.get(instance) == null)) {
                                val value: String = jsonObj.get(instance).toString
                                map.put(instance, value)
                            }
                            else map.put(instance, "")
                            //   map += (instance -> value)
                            //   map.put(instance, value)
                            //   println("===key====：" + instance + "===value===：" + value)
                        }
                    case _ =>
                }
            } catch {
                case e: Exception => {
                    println("异常数据==》  "+json_str)
                    e.printStackTrace()
                }
            }
        }
        map
    }

//    def jsonToEV_log(json_str: String) = {
//        val gson: Gson = new Gson()
//        val eVLog_gson: EVLog_gson = gson.fromJson(json_str, classOf[EVLog_gson])
//
//        val ev_len: Int = eVLog_gson.request_body.split("\":\"").length
//        if (ev_len == 11) {
//            val data_web_list: Data_web_list = gson.fromJson(eVLog_gson.request_body, classOf[Data_web_list])
//            EVLog_web(eVLog_gson.url, eVLog_gson.remote_addr, eVLog_gson.time_local, eVLog_gson.http_host, eVLog_gson.request, eVLog_gson.http_status, eVLog_gson.referer, eVLog_gson.user_agent, data_web_list, eVLog_gson.app_key, eVLog_gson.app_channel, eVLog_gson.app_version, eVLog_gson.device_id, eVLog_gson.protocol_addr, eVLog_gson.forwarded_addr, eVLog_gson.client_id)
//        } else if (ev_len == 13) {
//            val data_move_2_list: Data_move_2_list = gson.fromJson(eVLog_gson.request_body, classOf[Data_move_2_list])
//            EVLog_move2(eVLog_gson.url, eVLog_gson.remote_addr, eVLog_gson.time_local, eVLog_gson.http_host, eVLog_gson.request, eVLog_gson.http_status, eVLog_gson.referer, eVLog_gson.user_agent, data_move_2_list, eVLog_gson.app_key, eVLog_gson.app_channel, eVLog_gson.app_version, eVLog_gson.device_id, eVLog_gson.protocol_addr, eVLog_gson.forwarded_addr, eVLog_gson.client_id)
//        }
//        else if (ev_len >= 14) {
//
//            val data_arr: Data_arr_gson_move1 = gson.fromJson(eVLog_gson.request_body, classOf[Data_arr_gson_move1])
//            val data_list: util.List[Data_move_1] = new util.ArrayList[Data_move_1]()
//            for (data_gson <- data_arr.data) {
//                val actionParm: ActionParm = gson.fromJson(data_gson.actionParm, classOf[ActionParm])
//                data_list.add(Data_move_1(data_gson.cityId, data_gson.pageName, data_gson.viewId, data_gson.pageTitle, data_gson.authorization, data_gson.pageId, data_gson.actionTime, data_gson.actionId, data_gson.latitude, data_gson.longitude, data_gson.pvId, data_gson.actionName, actionParm, data_gson.actionType))
//            }
//            EVLog_move1(eVLog_gson.url, eVLog_gson.remote_addr, eVLog_gson.time_local, eVLog_gson.http_host, eVLog_gson.request, eVLog_gson.http_status, eVLog_gson.referer, eVLog_gson.user_agent, Data_move_1_list(data_list), eVLog_gson.app_key, eVLog_gson.app_channel, eVLog_gson.app_version, eVLog_gson.device_id, eVLog_gson.protocol_addr, eVLog_gson.forwarded_addr, eVLog_gson.client_id)
//
//        }
//    }

    //    def json2Map(json : String) : mutable.HashMap[String,Object] = {
    //
    //        val map : mutable.HashMap[String,Object]= mutable.HashMap()
    //
    //        val jsonParser: JSONParser =new JSONParser()
    //
    //        //将string转化为jsonObject
    //        val jsonObj: JSONObject = jsonParser.parse(json).asInstanceOf[JSONObject]
    //
    //        //获取所有键
    //        val jsonKey: util.Set[String] = jsonObj.keySet()
    //
    //        val iter: util.Iterator[String] = jsonKey.iterator()
    //
    //        while (iter.hasNext){
    //            val field: String = iter.next()
    //            val value = jsonObj.getOrElse(field,null).toString
    //
    //            if(value.startsWith("{")&&value.endsWith("}")){
    //                //val value1 = mapAsScalaMap(jsonObj.getOrElse(field,null).asInstanceOf[util.HashMap[String, String]])
    //                val value1 = json2Map(value)
    //                map.put(field,value1)
    //            }else{
    //                map.put(field,value)
    //            }
    //        }
    //        map
    //    }


    def main(args: Array[String]): Unit = {
//        val gson: Gson = new Gson()
//        val json: String = "{\"url\":\"https://open.jiehun.com.cn/user/sdk/post-web-data\",\"remote_addr\":\"114.242.249.90\",\"time_local\":\"1614062682.076\",\"http_host\":\"open.jiehun.com.cn\",\"request\":\"POST /user/sdk/post-web-data HTTP/2.0\",\"http_status\":\"200\",\"referer\":\"https://expo.jiehun.com.cn/channel?src=GRZX&uid=toutu\",\"user_agent\":\"Mozilla/5.0 (iPhone; CPU iPhone OS 14_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148<<a=hunbasha_ios&p=ciw&m=3>> <[[on=iOS&ov=14.3&dn=iPhone12,5&m=Apple&r=2688X1242]]>\",\"request_body\":\"{\\\"data\\\":[{\\\"cityId\\\":\\\"110900\\\",\\\"pageName\\\":\\\"https://expo.jiehun.com.cn/channel?src=GRZX&uid=toutu\\\",\\\"pageTitle\\\":\\\"中国婚博会-展会频道\\\",\\\"viewId\\\":\\\"D5A10959-9226-4547-8659-FA558D52728B\\\",\\\"authorization\\\":\\\"dmp ARRNOLiM9bsNBlB6eyz6FFYUTSK53JDnWRZceX0q+RtMAg1o8Iyu7V5bEyI5R6ZJWAwJaO6a/bcLA0puOXGiSFgMCWftmPm7CQJQdWE6pl1YDBpg7ZrhvAkGSH55IeEUShRFd72W+bdeVwR9fXv8HkxUATW9zau6ClBWey4trU9IBwFn\\\",\\\"pageId\\\":\\\"260f7881-2926-98c1-8ad4-01078dfa6d2f\\\",\\\"actionTime\\\":\\\"1614062681918\\\",\\\"actionId\\\":\\\"a77b7d9e-24c2-bc6c-b83c-171d2896efad\\\",\\\"latitude\\\":\\\"39.982954\\\",\\\"longitude\\\":\\\"116.444688\\\",\\\"pvId\\\":\\\"44a8d0c9-6faa-9792-974d-a73c08b0d48a\\\",\\\"actionName\\\":\\\"expo_home\\\",\\\"actionParm\\\":\\\"{\\\\\\\"blockName\\\\\\\":\\\\\\\"底部固定按钮\\\\\\\",\\\\\\\"blockIndex\\\\\\\":11,\\\\\\\"itemName\\\\\\\":\\\\\\\"我的邀请函\\\\\\\",\\\\\\\"status\\\\\\\":1}\\\",\\\"actionType\\\":\\\"tap\\\"}]}\",\"app_key\":\"hunbasha_ios\",\"app_channel\":\"App Store\",\"app_version\":\"7.17.0\",\"device_id\":\"7DAC66FA-BE54-4A04-B9CE-7146C7D22219\",\"protocol_addr\":\"114.242.249.90\",\"forwarded_addr\":\"\",\"client_id\":\"7DAC66FA-BE54-4A04-B9CE-7146C7D22219\"}"
//        val json_web: String = "{\"url\":\"https://open.jiehun.com.cn/user/sdk/post-web-data\",\"remote_addr\":\"223.104.63.103\",\"time_local\":\"1559618832.650\",\"http_host\":\"open.jiehun.com.cn\",\"request\":\"POST /user/sdk/post-web-data HTTP/1.1\",\"http_status\":\"200\",\"referer\":\"https://expo.yingbasha.com/m/guangzhou/?src=gzmyff0531zh-cpc-zhcwp-gs001sp&uid=zhihudsp\",\"user_agent\":\"_ZhihuHybrid osee2unifiedRelease/1330 osee2unifiedReleaseVersion/4.44.0 Mozilla/5.0 (iPhone; CPU iPhone OS 12_3_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148\",\"request_body\":\"{\\\"data\\\":[{\\\"authorization\\\":\\\"\\\",\\\"cityId\\\":440100,\\\"visitCityName\\\":\\\"\\\",\\\"pageId\\\":\\\"f993c1d1-8781-a251-80f4-b1a1a225be5f\\\",\\\"pageName\\\":\\\"https://expo.yingbasha.com/m/guangzhou/?src=gzmyff0531zh-cpc-zhcwp-gs001sp&uid=zhihudsp\\\",\\\"pageTitle\\\":\\\"广州婴芭莎·儿博会 2019时间/地址/门票 - 中国婴芭莎官网\\\",\\\"actionTime\\\":\\\"1559618831752\\\",\\\"viewId\\\":\\\"\\\",\\\"longitude\\\":\\\"\\\",\\\"latitude\\\":\\\"\\\",\\\"referer\\\":\\\"\\\"}]}\",\"app_key\":\"hunbasha_pc\",\"app_channel\":\"\",\"app_version\":\"\",\"device_id\":\"\",\"protocol_addr\":\"223.104.63.103\",\"forwarded_addr\":\"\",\"client_id\":\"90f23d8dd0e530b1a7089bd88051da83\"}"
//
//        //   val data_web: EVLog_web = gson.fromJson(json_web, classOf[EVLog_web])
//
//
//        println(jsonToMap(json_web))
//
//        println(jsonToMap("{\n        \"data\":[\n            {\n                \"authorization\":\"\",\n                \"cityId\":440100,\n                \"visitCityName\":\"\",\n                \"pageId\":\"f993c1d1-8781-a251-80f4-b1a1a225be5f\",\n                \"pageName\":\"https://expo.yingbasha.com/m/guangzhou/?src=gzmyff0531zh-cpc-zhcwp-gs001sp&uid=zhihudsp\",\n                \"pageTitle\":\"广州婴芭莎·儿博会 2019时间/地址/门票 - 中国婴芭莎官网\",\n                \"actionTime\":\"1559618831752\",\n                \"viewId\":\"\",\n                \"longitude\":\"\",\n                \"latitude\":\"\",\n                \"referer\":\"\"\n            }\n        ]\n    }"))
//
//        // println(JsonUtil.jsonToEV_log(json_web))

        //  println(gson.fromJson(data_web.request_body,classOf[Data_web_list]))


        //     println( jsonToEV_log(json))
        //   println(new Gson().fromJson(json,classOf[EVLog_move1]))


        //   val test: String = "{\"url\":\"https://open.jiehun.com.cn/user/sdk/post-web-data\",\"remote_addr\":\"223.104.191.154\",\"time_local\":\"1614074504.606\",\"http_host\":\"open.jiehun.com.cn\",\"request\":\"POST /user/sdk/post-web-data HTTP/2.0\",\"http_status\":\"200\",\"referer\":\"https://m.jiehun.com.cn/baike/article2062/\",\"user_agent\":\"Mozilla/5.0 (Linux; Android 9; vivo X21A Build/PKQ1.180819.001; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/72.0.3626.121 Mobile Safari/537.36 SCore/362 SGInfo/1080/2075/3.0 SogouSearch Android1.0 version3.0 AppVersion/7871 NoHead\",\"request_body\":\"{\\\"data\\\":[{\\\"cityId\\\":\\\"0\\\",\\\"pageName\\\":\\\"https://m.jiehun.com.cn/baike/article2062/\\\",\\\"pageTitle\\\":\\\"临沂费县民政局婚姻登记处 上班时间/需要材料/地址/电话 - 中国婚博会官网\\\",\\\"pageId\\\":\\\"ac6d0aaf-ddec-4c38-adb0-3653927f13dd\\\",\\\"actionTime\\\":\\\"1614074504554\\\",\\\"actionId\\\":\\\"0f3de26f-5cfd-b884-bbfe-6f3f21460aeb\\\",\\\"latitude\\\":\\\"0.000000\\\",\\\"longitude\\\":\\\"0.000000\\\",\\\"referer\\\":\\\"https://m.sogou.com/web/searchList.jsp?noires=true&pid=sogou-clse-ddcbe25988981920-0042&uID=516b869497033971936&mid=516b869497033971936&xid=78b8260bd40290b45eadb164ab6c95e489fe&keyword=%E8%B4%B9%E5%8E%BF%E6%B0%91%E6%94%BF%E5%B1%80%E4%B8%8A%E7%8F%AD%E6%97%B6%E9%97%B4&cchn=3057&achn=3057&dp=1\\\",\\\"pvId\\\":\\\"a6563e2a-07ba-be84-a378-ffad773181ee\\\",\\\"actionParm\\\":\\\"{}\\\",\\\"actionType\\\":\\\"page\\\"}]}\",\"app_key\":\"hunbasha_wap\",\"app_channel\":\"\",\"app_version\":\"1.3.0\",\"device_id\":\"\",\"protocol_addr\":\"223.104.191.154\",\"forwarded_addr\":\"\",\"client_id\":\"4b33965a328f4397a49899fde8e74856\"}"

        //  val vLog_gson: EVLog_gson = new Gson().fromJson(test, classOf[EVLog_gson])

        //        println(vLog_gson.request_body)
        //        println(vLog_gson.request_body.replace("\":\"", "----------")
        //
        //        )
        //
        //        println(vLog_gson.request_body.split("\":\"").length)

    }


}
