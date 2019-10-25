package com.rethinkdb.model;

import org.jetbrains.annotations.Nullable;

import java.util.List;

public class Profile {

    private List<Object> profileObj;

    private Profile(List<Object> profileObj) {
        this.profileObj = profileObj;
    }

    public static @Nullable Profile fromList(List<Object> profileObj) {
        if(profileObj == null || profileObj.size() == 0){
            return null;
        }
        return new Profile(profileObj);
    }

    public List<Object> getProfileObj() {
        return profileObj;
    }
}
