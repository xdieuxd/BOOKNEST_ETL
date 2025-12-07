package com.booknest.etl.messaging.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import com.booknest.etl.dto.EtlResultDto;
import com.booknest.etl.logging.EtlLog;
import com.booknest.etl.logging.EtlLogRepository;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class ErrorMessageListener {

    private static final Logger log = LoggerFactory.getLogger(ErrorMessageListener.class);

    private final EtlLogRepository etlLogRepository;


    public void consumeError(EtlResultDto result) {
        log.warn("Error result received for entity {}:{} -> {}", result.getEntityType(), result.getEntityKey(), result.getErrors());
        etlLogRepository.save(EtlLog.builder()
                .jobName("DATA_QUALITY")
                .stage("QUALITY")
                .status("FAILED")
                .message(result.getMessage())
                .sourceRecord(result.getEntityType() + ":" + result.getEntityKey())
                .startedAt(result.getProcessedAt())
                .finishedAt(result.getProcessedAt())
                .build());
    }
}
