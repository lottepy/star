package com.magnumresearch.aqumon.riskengine.adapter.boci.feign;

import com.magnumresearch.aqumon.common.pojo.BOCIResult;
import com.magnumresearch.aqumon.riskengine.adapter.boci.config.BOCIFeignConfig;
import com.magnumresearch.aqumon.riskengine.adapter.boci.pojo.BOCIOrder;
import com.magnumresearch.aqumon.riskengine.adapter.boci.pojo.BOCIPortfolio;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;

@Component
@ConditionalOnProperty(prefix = "adapter.boci", value = {"enabled"})
@FeignClient(name = "boci-api", configuration = BOCIFeignConfig.class, url = "${adapter.boci.address}")
@RequestMapping("/api")
public interface BOCIFeignClient {
    @RequestMapping(value = "/account/{accountId}", method = RequestMethod.GET,
            headers = {"Accept=application/json", "caller_id=hkdm\\aqumon", "username=hkdm\\aqumon"})
    BOCIResult<BOCIPortfolio> enquirePortfolio(@PathVariable("accountId") String accountId);

    @RequestMapping(value = "/trade/order/{orderCode}", method = RequestMethod.GET,
            headers = {"Accept=application/json", "caller_id=hkdm\\aqumon", "username=hkdm\\aqumon"})
    BOCIResult<HashMap<String, BOCIOrder>> queryOrder(@PathVariable("orderCode") String orderCode);

    @RequestMapping(value = "/trade/orderbook/enquire/{accountId}", method = RequestMethod.GET,
            headers = {"Accept=application/json", "caller_id=hkdm\\aqumon", "username=hkdm\\aqumon"})
    BOCIResult<HashMap<String, List<BOCIOrder>>> queryOrderBook(@PathVariable("accountId") String accountId);

}
