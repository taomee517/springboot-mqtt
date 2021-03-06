/**
 * Copyright (c) 2018, Mr.Wang (recallcode@aliyun.com) All rights reserved.
 */

package org.example.mqtt.config;

import org.apache.commons.lang3.StringUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 自动配置apache ignite
 */
@Configuration
public class IgniteAutoConfig {

	@Autowired
	IgniteProperties igniteProperties;


	@Bean
	public Ignite ignite() throws Exception {
		IgniteConfiguration igniteConfiguration = new IgniteConfiguration();
		// Ignite实例名称
		igniteConfiguration.setIgniteInstanceName(igniteProperties.getBrokerId());
		// Ignite日志
//		Logger logger = LoggerFactory.getLogger("org.apache.ignite");
//		igniteConfiguration.setGridLogger(new Slf4jLogger(logger));
		// 非持久化数据区域
		DataRegionConfiguration notPersistence = new DataRegionConfiguration().setPersistenceEnabled(false)
			.setInitialSize(igniteProperties.getNotPersistenceInitialSize() * 1024 * 1024)
			.setMaxSize(igniteProperties.getNotPersistenceMaxSize() * 1024 * 1024).setName("not-persistence-data-region");
		// 持久化数据区域
		DataRegionConfiguration persistence = new DataRegionConfiguration().setPersistenceEnabled(true)
			.setInitialSize(igniteProperties.getPersistenceInitialSize() * 1024 * 1024)
			.setMaxSize(igniteProperties.getPersistenceMaxSize() * 1024 * 1024).setName("persistence-data-region");
		DataStorageConfiguration dataStorageConfiguration = new DataStorageConfiguration().setDefaultDataRegionConfiguration(notPersistence)
			.setDataRegionConfigurations(persistence)
			.setWalArchivePath(StringUtils.isNotBlank(igniteProperties.getPersistenceStorePath()) ? igniteProperties.getPersistenceStorePath() : null)
			.setWalPath(StringUtils.isNotBlank(igniteProperties.getPersistenceStorePath()) ? igniteProperties.getPersistenceStorePath() : null)
			.setStoragePath(StringUtils.isNotBlank(igniteProperties.getPersistenceStorePath()) ? igniteProperties.getPersistenceStorePath() : null);
		igniteConfiguration.setDataStorageConfiguration(dataStorageConfiguration);
		// 集群, 基于组播或静态IP配置
		TcpDiscoverySpi tcpDiscoverySpi = new TcpDiscoverySpi();
		if (igniteProperties.isEnableMulticastGroup()) {
			TcpDiscoveryMulticastIpFinder tcpDiscoveryMulticastIpFinder = new TcpDiscoveryMulticastIpFinder();
			tcpDiscoveryMulticastIpFinder.setMulticastGroup(igniteProperties.getMulticastGroup());
			tcpDiscoverySpi.setIpFinder(tcpDiscoveryMulticastIpFinder);
		} else {
			TcpDiscoveryVmIpFinder tcpDiscoveryVmIpFinder = new TcpDiscoveryVmIpFinder();
			tcpDiscoveryVmIpFinder.setAddresses(igniteProperties.getStaticIpAddresses());
			tcpDiscoverySpi.setIpFinder(tcpDiscoveryVmIpFinder);
		}
		igniteConfiguration.setDiscoverySpi(tcpDiscoverySpi);
		Ignite ignite = Ignition.start(igniteConfiguration);
		ignite.cluster().active(true);
		return ignite;
	}

	@Bean
	public IgniteCache messageIdCache() throws Exception {
		CacheConfiguration cacheConfiguration = new CacheConfiguration().setDataRegionName("not-persistence-data-region")
			.setCacheMode(CacheMode.PARTITIONED).setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL).setName("messageIdCache");
		return ignite().getOrCreateCache(cacheConfiguration);
	}

	@Bean
	public IgniteCache retainMessageCache() throws Exception {
		CacheConfiguration cacheConfiguration = new CacheConfiguration().setDataRegionName("persistence-data-region")
			.setCacheMode(CacheMode.PARTITIONED).setName("retainMessageCache");
		return ignite().getOrCreateCache(cacheConfiguration);
	}

	@Bean
	public IgniteCache subscribeNotWildcardCache() throws Exception {
		CacheConfiguration cacheConfiguration = new CacheConfiguration().setDataRegionName("persistence-data-region")
			.setCacheMode(CacheMode.PARTITIONED).setName("subscribeNotWildcardCache");
		return ignite().getOrCreateCache(cacheConfiguration);
	}

	@Bean
	public IgniteCache subscribeWildcardCache() throws Exception {
		CacheConfiguration cacheConfiguration = new CacheConfiguration().setDataRegionName("persistence-data-region")
			.setCacheMode(CacheMode.PARTITIONED).setName("subscribeWildcardCache");
		return ignite().getOrCreateCache(cacheConfiguration);
	}

	@Bean
	public IgniteCache dupPublishMessageCache() throws Exception {
		CacheConfiguration cacheConfiguration = new CacheConfiguration().setDataRegionName("persistence-data-region")
			.setCacheMode(CacheMode.PARTITIONED).setName("dupPublishMessageCache");
		return ignite().getOrCreateCache(cacheConfiguration);
	}

	@Bean
	public IgniteCache dupPubRelMessageCache() throws Exception {
		CacheConfiguration cacheConfiguration = new CacheConfiguration().setDataRegionName("persistence-data-region")
			.setCacheMode(CacheMode.PARTITIONED).setName("dupPubRelMessageCache");
		return ignite().getOrCreateCache(cacheConfiguration);
	}

	@Bean
	public IgniteMessaging igniteMessaging() throws Exception {
		return ignite().message(ignite().cluster().forRemotes());
	}

}
