package com.epam.jmp.redislab.service;

import com.epam.jmp.redislab.api.RequestDescriptor;
import com.epam.jmp.redislab.configuration.ratelimit.RateLimitRule;
import com.epam.jmp.redislab.configuration.ratelimit.RateLimitTimeInterval;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;
import redis.clients.jedis.JedisCluster;

import java.time.LocalTime;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Component
public class JedisRateLimitService implements RateLimitService {

    private static final long SECONDS_IN_MINUTE = 60;
    private static final long SECONDS_IN_HOUR = 3600;
    private static final String KEY_DELIMITER = ":";

    private final JedisCluster jedisCluster;
    private final Collection<RateLimitRule> rateLimitRules;

    public JedisRateLimitService(JedisCluster jedisCluster, Collection<RateLimitRule> rateLimitRules) {
        this.jedisCluster = jedisCluster;
        this.rateLimitRules = rateLimitRules;
    }

    @Override
    public boolean shouldLimit(Set<RequestDescriptor> requestDescriptors) {
        LocalTime startTime = LocalTime.now();

        for (RequestDescriptor requestDescriptor : requestDescriptors) {
            Optional<RateLimitRule> matchingRule = findMatchingRule(requestDescriptor);

            if (!matchingRule.isPresent()) {
                continue;
            }

            RateLimitRule rateLimitRule = matchingRule.get();
            String descriptorKeyName = buildDescriptorKeyName(requestDescriptor,
                    rateLimitRule.getTimeInterval(), startTime);

            boolean isDescriptorKeyExists = jedisCluster.exists(descriptorKeyName);
            if (isDescriptorKeyExists) {
                long numberOfRequests = jedisCluster.incr(descriptorKeyName);
                if (numberOfRequests > rateLimitRule.getAllowedNumberOfRequests()) {
                    return true;
                }
            } else {
                long ttl = rateLimitRule.getTimeInterval().equals(RateLimitTimeInterval.MINUTE)
                        ? SECONDS_IN_MINUTE
                        : SECONDS_IN_HOUR;
                jedisCluster.setex(descriptorKeyName, ttl, "1");
            }
        }

        return false;
    }

    private Optional<RateLimitRule> findMatchingRule(RequestDescriptor requestDescriptor) {
        List<RateLimitRule> matchingRules = rateLimitRules.stream()
                .filter(rule -> {
                    if (isRuleFieldMatchesDescriptor(rule.getAccountId(), requestDescriptor.getAccountId())) {
                        return false;
                    }

                    if (isRuleFieldMatchesDescriptor(rule.getClientIp(), requestDescriptor.getClientIp())) {
                        return false;
                    }

                    return !isRuleFieldMatchesDescriptor(rule.getRequestType(), requestDescriptor.getRequestType());
                })
                .collect(Collectors.toList());

        if (matchingRules.size() == 0) {
            return Optional.empty();
        }

        RateLimitRule matchingRule = matchingRules.iterator().next();
        for (int i = 1; i < matchingRules.size(); ++i) {
            RateLimitRule currentRule = matchingRules.get(i);
            if (isRuleMoreAccurate(currentRule, matchingRule)) {
                matchingRule = currentRule;
            }
        }

        return Optional.of(matchingRule);
    }

    private boolean isRuleFieldMatchesDescriptor(Optional<String> ruleField, Optional<String> descriptorField) {
        return ruleField.isPresent() && StringUtils.isNotBlank(ruleField.get())
                && (!descriptorField.isPresent()
                || !ruleField.get().equals(descriptorField.get()));
    }

    /**
     * Detects if rule1 is more accurate than rule2 based on fields content
     *
     * @param rule1
     * @param rule2
     * @return true, if rule1 more accurate than rule2
     * false, otherwise
     */
    private boolean isRuleMoreAccurate(RateLimitRule rule1, RateLimitRule rule2) {
        boolean isAccountIdMoreAccurate = rule1.getAccountId().isPresent()
                && (!rule2.getAccountId().isPresent() || StringUtils.isBlank(rule2.getAccountId().get()));
        boolean isClientIpMoreAccurate = rule1.getClientIp().isPresent()
                && (!rule2.getClientIp().isPresent() || StringUtils.isBlank(rule2.getClientIp().get()));
        boolean isRequestTypeMoreAccurate = rule1.getRequestType().isPresent()
                && (!rule2.getRequestType().isPresent() || StringUtils.isBlank(rule2.getRequestType().get()));

        return isAccountIdMoreAccurate || isClientIpMoreAccurate || isRequestTypeMoreAccurate;
    }

    private String buildDescriptorKeyName(RequestDescriptor requestDescriptor,
                                          RateLimitTimeInterval rateLimitTimeInterval, LocalTime startTime) {
        StringBuilder keyNameBuilder = new StringBuilder("request");

        requestDescriptor.getRequestType().ifPresent(s -> {
            if (StringUtils.isNotBlank(s)) {
                keyNameBuilder.append(KEY_DELIMITER).append(s);
            }
        });
        requestDescriptor.getAccountId().ifPresent(s -> {
            if (StringUtils.isNotBlank(s)) {
                keyNameBuilder.append(KEY_DELIMITER).append("user.").append(s);
            }
        });
        requestDescriptor.getClientIp().ifPresent(s -> {
            if (StringUtils.isNotBlank(s)) {
                keyNameBuilder.append(KEY_DELIMITER).append(s);
            }
        });

        keyNameBuilder.append(KEY_DELIMITER)
                .append(rateLimitTimeInterval.name())
                .append(KEY_DELIMITER)
                .append(rateLimitTimeInterval.equals(RateLimitTimeInterval.MINUTE)
                        ? startTime.getMinute()
                        : startTime.getHour());

        return keyNameBuilder.toString();
    }
}
