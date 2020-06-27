package com.zhengxw.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zhengxw.gmall.common.constants.GmallConstant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@Slf4j
public class LoggerController {
    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;

    @PostMapping("/log")
    @ResponseBody
    public String log(@RequestParam("logString") String logString){
        JSONObject jsonObject = JSON.parseObject(logString);
        jsonObject.put("ts",System.currentTimeMillis());

        log.info(logString);

        if("startup".equals(jsonObject.get("type"))){
            kafkaTemplate.send(GmallConstant.KAFKA_STARTUP,jsonObject.toJSONString());
        }else{
            kafkaTemplate.send(GmallConstant.KAFKA_EVENT,jsonObject.toJSONString());
        }

        System.out.println(logString);
        return "success";
    }

}
