package com.epam.data.engineering.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Weather {
    private Double lng;
    private Double lat;
    private Double avg_tmpr_f;
    private Double avg_tmpr_c;
    private String wthr_date;
    private String year;
    private String month;
    private String day;
}
