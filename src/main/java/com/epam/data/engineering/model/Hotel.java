package com.epam.data.engineering.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Hotel {
    private String id;
    private String name;
    private String country;
    private String city;
    private String address;
    private String latitude;
    private String longitude;
}
