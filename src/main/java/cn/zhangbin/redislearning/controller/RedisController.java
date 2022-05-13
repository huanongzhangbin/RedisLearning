package cn.zhangbin.redislearning.controller;

import jdk.nashorn.internal.runtime.logging.Logger;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;
import java.util.concurrent.TimeUnit;

@Slf4j
@RestController()
public class RedisController {

    @Autowired
    RedisTemplate redisTemplate;

    @GetMapping("/testRedis")
    public String testRedis() {

        redisTemplate.opsForValue().set("name", "jack");
        String s = (String) redisTemplate.opsForValue().get("name");
        return s;
    }

    /**
     * @param goodId
     * @param userId
     * @return
     * @description 目前还存在库存遗留问题。可使用lua脚本解决
     */
    @GetMapping("/doSecondKill")
    public HashMap<String, Object> doSecondKill(@RequestParam("goodId") String goodId, @RequestParam("userId") String userId) {
        String kcKey = "kcKey" + goodId;
        String userKey = "win:" + goodId + "userId:" + userId;
        HashMap<String, Object> res = new HashMap<>();
        redisTemplate.execute(new SessionCallback() {
            @Override
            public Object execute(RedisOperations redisOperations) throws DataAccessException {
                redisOperations.watch(kcKey);//开启乐观锁监视库存
                Object value = redisOperations.opsForValue().get(kcKey);
                if (null == value) {
                    res.put("msg", "秒杀未开始");
                    res.put("code", 0);
                    return null;
                }
                Integer amount = (Integer) value;
                if (amount <= 0) {
                    res.put("msg", "秒杀结束，抢购失败");
                    res.put("code", 0);
                    return null;
                }
                if (redisOperations.opsForSet().isMember(userKey, userId)) {
                    res.put("msg", "已经秒杀成功啦，不能再次抢购啦");
                    res.put("code", 0);
                    return null;
                }
                redisOperations.multi();//开启事务操作，使秒杀成功者进入set
                redisOperations.opsForValue().decrement(kcKey);
                redisOperations.opsForSet().add(userKey, userId);
                List<Object> list = redisOperations.exec();
                if (list == null || list.size() == 0) {
                    res.put("msg", "秒杀失败");
                    res.put("code", 0);
                    return null;
                }
                res.put("msg", "秒杀成功啦");
                res.put("code", 1);
                return null;
            }
        });
        return res;

    }

    /**
     * @param goodId
     * @param userId
     * @return
     * @description 这段脚本还有问题，会报错
     */
    @GetMapping("/doSecondKill2")
    public String doSecondKill2(@RequestParam("goodId") String goodId, @RequestParam("userId") String userId) {
        String lua = "local goodId=KEYS[1];\n" +
                "local userId=KEYS[1];\n" +
                "local kcKey='kcKey'..goodId;\n" +
                "local winUserKey='win-'..goodId..'user-'..userId;\n" +
                "val=redis.call('get',kcKey);\n" +
                "if val==ngx.null then\n" +
                "    return 0;\n" +
                "elseif tonumber(val)<=0 then\n" +
                "    return 1;\n" +
                "else\n" +
                "     isexist=redis.call('sismember',winUserKey)\n" +
                "     if isexist==1 then\n" +
                "        return 2;\n" +
                "     else\n" +
                "        redis.call('decr',kcKey);\n" +
                "        redis.call('sadd',winUserKey);\n" +
                "        return 3;\n" +
                "     end\n" +
                "end";

        RedisScript redisScript = new DefaultRedisScript(lua, String.class);

        String result = (String) redisTemplate.execute(redisScript, Arrays.asList(goodId, userId));
        if ("0".equals(result)) {
            return "秒杀还没开始";
        }
        if ("1".equals(result)) {
            return "秒杀已经结束了";
        }
        if ("2".equals(result)) {
            return "已经秒杀过了";
        }
        if ("3".equals(result)) {
            return "秒杀成功了";
        }
        return "秒杀成功了";
    }

    /**
     * @description  使用分布式锁，setnx来实现超卖问题，
     * @param goodId 商品id,
     * @param userId 用户id
     * @return
     */
    @GetMapping("/doSecondKill3")
    public Map<String, Object> doSecondKill3(@RequestParam("goodId") String goodId, @RequestParam("userId") String userId) {

        String goodKey = "goodKey" + goodId;//goodKey，获得此key将获得分布式锁
        log.info(goodKey);
        String winnerSet=goodId+"winnerSet";//抢购成功的用户
        String elementUser= "element" + userId;//带加入winnerSet中的用户
        String goodNumKey=goodId+"num";//库存key
        String uuid = String.valueOf(UUID.randomUUID());//value设置为uuid防止误删
        HashMap<String, Object> res = new HashMap<>();
        /**
         * 先通过一轮查询判断库存，没库存，没必要为了获得锁进入sleep
         */
        Object num= redisTemplate.opsForValue().get(goodNumKey);
        if(null==num){
            res.put("code",0);
            res.put("msg","未设置库存数量");
            return res;
        }else if((Integer)num<=0){
            res.put("code",0);
            res.put("msg","没有库存了");
            return res;
        }
        /**
         * 第一轮前置判断解锁
         */

        Boolean lock = redisTemplate.opsForValue().setIfAbsent(goodKey, uuid, 1, TimeUnit.SECONDS);//尝试获得锁，如果获得，设置过期时间未30s
        log.info("lock :"+lock);
        if (lock) {//如果获得锁
            num = redisTemplate.opsForValue().get(goodNumKey);
            if (StringUtils.isEmpty(num)){
                res.put("code",0);
                res.put("msg","未设置库存数量");
                return res;
            }
            //下面开始业务处理。
           //无库存
            if((Integer)num<=0){//库存未0，已经结束啦
                res.put("code",0);
                res.put("msg","没有库存了");
            }else if(redisTemplate.opsForSet().isMember(winnerSet,elementUser)){//有库存,但是已经抢购过
                res.put("code",0);
                res.put("msg","你已经抢购过了，不能再次抢购");
            }else{
                //有库存，还没有抢购过
                redisTemplate.opsForValue().decrement(goodNumKey);//个数--
                redisTemplate.opsForSet().add(winnerSet,elementUser);//抢购成功用户加入set。
                res.put("code",1);
                res.put("msg","抢购成功了");
            }
            //开始解锁
            String script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
            // 使用redis执行lua执行
            DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>();
            redisScript.setScriptText(script);
            // 设置一下返回值类型 为Long
            // 因为删除判断的时候，返回的0,给其封装为数据类型。如果不封装那么默认返回String 类型，
            // 那么返回字符串与0 会有发生错误。
            redisScript.setResultType(Long.class);
            redisTemplate.execute(redisScript, Arrays.asList(goodKey), uuid);//使用lua脚本解锁，防止手动解锁过程中，过期自动解锁插队导致解掉别人正在使用的锁。
            return res;

        }
        else {//未获得锁，则等待重试
            try {
                Thread.sleep(500);
              return  doSecondKill3(goodId, userId);
            } catch (InterruptedException e) {
                log.info(e.getMessage());
            }
        }

        return res;
    }

    @GetMapping("testLockLua")
    public void testLockLua() {
        //1 声明一个uuid ,将做为一个value 放入我们的key所对应的值中
        String uuid = UUID.randomUUID().toString();
        //2 定义一个锁：lua 脚本可以使用同一把锁，来实现删除！
        String skuId = "25"; // 访问skuId 为25号的商品 100008348542
        String locKey = "lock:" + skuId; // 锁住的是每个商品的数据

        // 3 获取锁
        Boolean lock = redisTemplate.opsForValue().setIfAbsent(locKey, uuid, 3, TimeUnit.SECONDS);

        // 第一种： lock 与过期时间中间不写任何的代码。
        // redisTemplate.expire("lock",10, TimeUnit.SECONDS);//设置过期时间
        // 如果true
        if (lock) {
            // 执行的业务逻辑开始
            // 获取缓存中的num 数据
            Object value = redisTemplate.opsForValue().get("num");
            // 如果是空直接返回
            if (StringUtils.isEmpty(value)) {
                return;
            }
            // 不是空 如果说在这出现了异常！ 那么delete 就删除失败！ 也就是说锁永远存在！
            int num = Integer.parseInt(value + "");
            // 使num 每次+1 放入缓存
            redisTemplate.opsForValue().set("num", String.valueOf(++num));
            /*使用lua脚本来锁*/
            // 定义lua 脚本
            String script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
            // 使用redis执行lua执行
            DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>();
            redisScript.setScriptText(script);
            // 设置一下返回值类型 为Long
            // 因为删除判断的时候，返回的0,给其封装为数据类型。如果不封装那么默认返回String 类型，
            // 那么返回字符串与0 会有发生错误。
            redisScript.setResultType(Long.class);
            // 第一个要是script 脚本 ，第二个需要判断的key，第三个就是key所对应的值。
            redisTemplate.execute(redisScript, Arrays.asList(locKey), uuid);
        } else {
            // 其他线程等待
            try {
                // 睡眠
                Thread.sleep(1000);
                // 睡醒了之后，调用方法。
                testLockLua();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
