package com.booknest.etl.dto;

import java.time.OffsetDateTime;
import java.util.List;

import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class UserRawMessage {
    String source;
    String userId;
    String fullName;
    String email;
    String phone;
    String status;
    List<String> roles;
    OffsetDateTime extractedAt;
}
