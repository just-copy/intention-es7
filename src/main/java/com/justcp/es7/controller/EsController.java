package com.justcp.es7.controller;

import com.justcp.es7.service.EsService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

@RestController
@RequestMapping("/es")
public class EsController {

    @Resource
    private EsService esService;

    @GetMapping(value = "/insert")
    public void insertEs() {
        esService.insertBatch();
    }
}
