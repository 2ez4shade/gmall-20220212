package com.shade.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author: shade
 * @date: 2022/6/25 22:41
 * @description:
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
public class Student {
    private String class_id;
    private String stu_id;
    private String name;
    private String sex;
    private String age;
    private String favo;

    public String print(){
        return "----------------------------------------------------------"+"\nclass_id: "+class_id+"\n"+
        "stu_id: "+stu_id+"\n"+
        "name: "+name+"\n"+
        "sex: "+sex+"\n"+
        "age: "+age+"\n"+
        "favo: "+favo+"\n"+"----------------------------------------------------------";
    }
}


