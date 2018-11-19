package com.json.ruleengine.vo;

import java.util.HashMap;
import java.util.Map;

/**
 * @Classname People
 * @Date 2018/11/11 下午3:01
 * @Create by yaolihua
 * @Description
 */
public class People {

    private Map<String, Object> info = new HashMap<String, Object>();

    public Map<String, Object> getInfo() {
        return info;
    }

    public void setInfo(Map<String, Object> info) {
        this.info = info;
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public String name;
    public int age;
    public String sex;
}
