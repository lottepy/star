package com.magnumresearch.aqumon.riskengine.adapter.kuanrui.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.Arrays;

@Configuration
public class AccountSettings {

    private String ctpAccountLive = "120101898";
    private String ctpAccountPaper = "7120102728";
    private String ctpAccountsLive = "10950938,120101898";
    private String ctpAccountsPaper = "50000119,7120102728,135937,135849";

    @Value("${adapter.kuanrui.test.username}")
    private String krUsernamePaper;

    @Value("${adapter.kuanrui.test.pwd}")
    private String krPasswordPaper;

    @Value("${adapter.kuanrui.username}")
    private String krUsernameLive;

    @Value("${adapter.kuanrui.pwd}")
    private String krPasswordLive;

    @Value("${account.ctp.islive}")
    private boolean ctpIsLive;

    @Value("${account.kr.islive}")
    private boolean krIsLive;

    @Bean(name="CtpAccountLive")
    public String getCtpAccountLive(){
        return ctpAccountLive;
    }

    @Bean(name="CtpAccountPaper")
    public String getCtpAccountPaper(){
        return ctpAccountLive;
    }

    @Bean(name="CtpAccountsLive")
    public ArrayList<String> getCtpAccountsLive(){
        ArrayList<String> accounts = new ArrayList<String>(Arrays.asList(ctpAccountsLive.split(",")));
        return accounts;
    }

    @Bean(name="CtpAccountsPaper")
    public ArrayList<String> getCtpAccountsPaper(){
        ArrayList<String> accounts = new ArrayList<String>(Arrays.asList(ctpAccountsPaper.split(",")));
        return accounts;
    }

    @Bean(name="CtpAccountsActive")
    public ArrayList<String> getCtpAccountsActive(){
        if (ctpIsLive){
            return getCtpAccountsLive();
        } else{
            return  getCtpAccountsPaper();
        }
    }

    @Bean(name="CtpAccountActiveForSubscription")
    public String getCtpAccountActiveForSubscription(){
        if (ctpIsLive){
            return getCtpAccountLive();
        } else{
            return  getCtpAccountPaper();
        }
    }

    @Bean(name="KrAccountLive")
    public String getKrAccountLive(){
        return krUsernameLive;
    }

    @Bean(name="KrAccountPwdLive")
    public String getKrAccountPwdLive(){
        return krPasswordLive;
    }

    @Bean(name="KrAccountPaper")
    public String getKrAccountPaper(){
        return krUsernamePaper;
    }

    @Bean(name="KrAccountPwdPaper")
    public String getKrAccountPwdPaper(){
        return krPasswordPaper;
    }

    @Bean(name="KrAccountActive")
    public String getKrAccountActive(){
        if (krIsLive){
            return getKrAccountLive();
        } else{
            return  getKrAccountPaper();
        }
    }

    @Bean(name="KrAccountPasswordActive")
    public String getKrAccountPasswordActive(){
        if (krIsLive){
            return getKrAccountPwdLive();
        } else{
            return  getKrAccountPwdPaper();
        }
    }

    @Bean(name="KrAccountActiveForSubscription")
    public String getKrAccountActiveForSubscription(){
        if (krIsLive){
            return getKrAccountLive();
        } else{
            return  getKrAccountPaper();
        }
    }

    public boolean getKrIsLive()
    {
        return krIsLive;
    }

}
