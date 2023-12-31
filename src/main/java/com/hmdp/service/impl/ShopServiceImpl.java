package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryById(Long id) {

        //缓存穿透
//        Shop shop = queryWithPassThrough(id);

        //利用互斥锁解决缓存击穿问题
        Shop shop = queryWithMutex(id);

        //利用逻辑过期策略解决缓存击穿问题、
//        Shop shop = queryWithLogicalExpire(id);

        if (shop == null) {
            return Result.fail("店铺不存在！");
        }

        //返回结果
        return Result.ok(shop);
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    //利用逻辑过期策略解决缓存击穿问题
    public Shop queryWithLogicalExpire(Long id){
        //从redis查询商铺缓存
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isBlank(shopJson)) {
            //redis存在，则直接返回
//            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
//            return Result.ok(shop);
            return null; //理论上热点key会一直存在，此时为了程序的健壮性考虑，此处进行判断并返回空值
        }
        //命中,需要把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        Shop shop = JSONUtil.toBean(data, Shop.class);

        LocalDateTime expireTime = redisData.getExpireTime();
        //判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            //未过期，则直接直接返回店铺信息
            return shop;
        }
        //已过期，则需缓存重建，并获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        //成功获取锁，则开启独立线程，实现缓存重建
        if(isLock){
            CACHE_REBUILD_EXECUTOR.submit(()->{
                try {
                    //重建缓存
                    this.saveShop2Redis(id,20L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    //释放锁
                    unLock(lockKey);
                }
            });
        }
        //锁获取失败，则返回商铺信息(过期)
        return shop;
    }

    //利用互斥锁解决缓存击穿问题
    public Shop queryWithMutex(Long id){
        //从redis查询商铺缓存
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(shopJson)) {
            //redis存在，则直接返回
//            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
//            return Result.ok(shop);
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        //判断命中的是否是空值(不是null就是空字符串)
        if (shopJson != null) {
            //是空值，返回错误信息
//            return Result.fail("店铺不存在");
            return null;
        }
        //redis不存在，则查询数据库(缓存重建)
        //获取互斥锁
        String lockKey = "lock:shop:" + id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);
            if (!isLock) {
                //获取互斥锁失败，则休眠并重试
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            //获取互斥锁成功，则查询数据库，写入redis，释放锁
            shop = getById(id);
            Thread.sleep(200);//模拟缓存重建延迟时长
            if (shop == null) {
                //数据库不存在，将空值写入redis
                stringRedisTemplate
                        .opsForValue()
                        .set(key,"",RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES); //2min
                //数据库不存在，则返回错误
    //            return Result.fail("店铺不存在！");
                return null;
            }
            //数据库存在，则将数据存入redis，并设置有效期(redis超时剔除策略)
            stringRedisTemplate
                    .opsForValue()
                    .set(key,JSONUtil.toJsonStr(shop),RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            unLock(lockKey); //释放锁
        }
        //返回结果
//        return Result.ok(shop);
        return shop;
    }

    //利用主动更新和超时剔除解决缓存穿透问题
    public Shop queryWithPassThrough(Long id){
        //从redis查询商铺缓存
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(shopJson)) {
            //redis存在，则直接返回
//            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
//            return Result.ok(shop);
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        //判断命中的是否是空值(不是null就是空值)
        if (shopJson != null) {
            //是空字符串，返回错误信息
//            return Result.fail("店铺不存在");
            return null;
        }
        //redis不存在，则查询数据库
        Shop shop = getById(id);
        if (shop == null) {
            //数据库不存在，将空值写入redis
            stringRedisTemplate
                    .opsForValue()
                    .set(key,"",RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES); //2min
            //数据库不存在，则返回错误
//            return Result.fail("店铺不存在！");
            return null;
        }
        //数据库存在，则将数据存入redis，并设置有效期(redis超时剔除策略)
        stringRedisTemplate
                .opsForValue()
                .set(key,JSONUtil.toJsonStr(shop),RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);//30min
        //返回结果
//        return Result.ok(shop);
        return shop;
    }


    //获取锁
    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }
    //释放锁
    private void unLock(String key){
        stringRedisTemplate.delete(key);
    }

    //模拟热点数据预热(缓存重建)
    public void saveShop2Redis(Long id,Long expireSeconds) throws InterruptedException {
        //查询店铺数据
        Shop shop = getById(id);
        Thread.sleep(200);//此处模拟线程安全问题
        //封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        //写入redis
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY+id,JSONUtil.toJsonStr(redisData));
    }


    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺id不能为空");
        }
        //redis主动更新策略
        //更新数据库
        updateById(shop);
        //删除缓存
        stringRedisTemplate.delete(RedisConstants.CACHE_SHOP_KEY + id);
        return Result.ok();
    }
}
