package com.magnumresearch.aqumon.riskengine.service;

import com.magnumresearch.aqumon.common.feign.LarkRobotClient;
import com.magnumresearch.aqumon.riskengine.adapter.BaseBrokerAdapter;
import com.magnumresearch.aqumon.riskengine.adapter.ayers.AyersBrokerAdapter;
import com.magnumresearch.aqumon.riskengine.adapter.ib.IbBrokerAdapter;
import com.magnumresearch.aqumon.riskengine.adapter.kuanrui.KuanruiBrokerAdapter;
import com.magnumresearch.aqumon.riskengine.dao.BrokerAccountConfigDao;
import com.magnumresearch.aqumon.trading.constants.BrokerType;
import com.magnumresearch.aqumon.trading.exception.TradingException;
import com.magnumresearch.aqumon.trading.model.BrokerAccountConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Service
public class TradingManageService {

    @Autowired
    BrokerAdapterService brokerAdapterService;
    @Autowired(required = false)
    AyersBrokerAdapter ayersBrokerAdapter;
    @Autowired(required = false)
    IbBrokerAdapter ibBrokerAdapter;
    @Autowired
    BrokerAccountConfigDao brokerAccountConfigDao;
    @Autowired
    LarkRobotClient larkRobotClient;

    Logger log = LoggerFactory.getLogger(this.getClass());

    public void refreshIbBrokerAccountConfig() {
        disconnectAllIB();
        ibBrokerAdapter.refreshIbBrokerAccountConfig();
    }

    public void disconnect(BrokerType brokerType, String brokerAccount) throws TradingException {
        log.info("[RiskEngine][TradingManageService][Disconnect] broker disconnect start: BrokerAccount=" + brokerAccount);
        BaseBrokerAdapter adapter = brokerAdapterService.getAdapter(brokerType);
        try {
            adapter.disconnect(brokerAccount);
        } catch (TradingException e) {
            log.error("[RiskEngine][TradingManageService][Disconnect] broker cannot be disconnected: BrokerAccount=" + brokerType + brokerAccount);
            larkRobotClient.sendMessage("[RiskEngine][Disconnect] ERROR! Cannot disconnect => " + brokerType + brokerAccount);
            throw e;
        }
        log.info("[RiskEngine][TradingManageService][Disconnect] broker disconnect success: BrokerAccount=" + brokerAccount);
    }

    @Scheduled(cron = "0 0 6,20 * * ?")
    @PreDestroy
    public void disconnectAllIB() {
        Set<String> brokerAccountSet = new HashSet<>();
        List<BrokerAccountConfig> accountConfigList = brokerAccountConfigDao.findAll();
        for (BrokerAccountConfig accountConfig : accountConfigList) {
            brokerAccountSet.add(accountConfig.getBrokerAccount());
        }
        for (String brokerAccount : brokerAccountSet) {
            try {
                disconnect(BrokerType.IB, brokerAccount);
            } catch (Exception e) {
                log.error("[RiskEngine][TradingManageService][DisconnectAllIB] broker IB cannot be disconnected: BrokerAccount=" + brokerAccount, e);
            }
        }
    }

    @Scheduled(cron = "0 0 23 * * FRI", zone = "America/New_York")
    @ConditionalOnProperty(prefix = "adapter.ib", value = {"enabled"})
    public void ibSetMaintenance() throws TradingException {
        setMaintenance(BrokerType.IB);
    }

    @Scheduled(cron = "0 0 3 * * SAT", zone = "America/New_York")
    @ConditionalOnProperty(prefix = "adapter.ib", value = {"enabled"})
    public void ibUnsetMaintenance() throws TradingException {
        unsetMaintenance(BrokerType.IB);
    }

    public void setMaintenance(BrokerType brokerType) throws TradingException {
        BaseBrokerAdapter adapter = brokerAdapterService.getAdapter(brokerType);
        adapter.setInMaintenance(true);
    }

    public void unsetMaintenance(BrokerType brokerType) throws TradingException {
        BaseBrokerAdapter adapter = brokerAdapterService.getAdapter(brokerType);
        adapter.setInMaintenance(false);
    }

    @Scheduled(cron = "0 0 6 * * MON-SAT")
    @Scheduled(cron = "0 0 3 * * SUN")
    public void disconnectAyers() throws TradingException {
        // AYERS的每天6:00会重启
        // 系统在周二到周六的凌晨关闭连接
        disconnect(BrokerType.AYERS, null);
    }

    @Scheduled(cron = "0 15 6 * * ?")
    public void connectAyers() {
        // AYERS的5：55断开6:30连接
        // 系统在周一到周五的凌晨打开连接
        ayersBrokerAdapter.connect();
    }

    public void keepAlive() throws TradingException {
        KuanruiBrokerAdapter kuanruiBrokerAdapter = (KuanruiBrokerAdapter) brokerAdapterService.getAdapter(BrokerType.KR);
        kuanruiBrokerAdapter.keepAlive();
    }

    public Object checkSubtaskAvailabilityWithDM(int taskId) throws TradingException {
        KuanruiBrokerAdapter kuanruiBrokerAdapter = (KuanruiBrokerAdapter) brokerAdapterService.getAdapter(BrokerType.KR);
        return kuanruiBrokerAdapter.checkSubtaskAvailabilityWithDM(taskId);
    }
}
