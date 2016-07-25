package com.company;

import java.util.List;

/**
 * Created by wangyiran on 25/7/2016.
 */
public class DeptMap {
    Long deptId;
    String deptName;
    Integer userCount;

    public Long getDeptId() {
        return deptId;
    }

    public void setDeptId(Long deptId) {
        this.deptId = deptId;
    }

    public String getDeptName() {
        return deptName;
    }

    public void setDeptName(String deptName) {
        this.deptName = deptName;
    }

    public Integer getUserCount() {
        return userCount;
    }

    public void setUserCount(Integer userCount) {
        this.userCount = userCount;
    }
}
