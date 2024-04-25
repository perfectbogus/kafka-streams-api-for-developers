/*
 * ====================================================================================
 *
 * Copyright (c) 2005, 2024 Oracle Ⓡ and/or its affiliates. All rights reserved.
 *
 * ====================================================================================
 */

package com.learnkafkastreams.domain;

import java.time.LocalDateTime;

public record Greeting(
    String message,
    LocalDateTime timestamp
) {
}
