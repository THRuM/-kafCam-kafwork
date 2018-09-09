package org.app.mongo;

import org.app.domain.Currency;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.math.BigDecimal;
import java.util.Date;

@Document
public class CurrencyData {
    @Id
    private String mongoId;
    private String requestId;
    private String symbol;
    private BigDecimal bid,
            ask,
            price;
    private Long time;
    private Date mongoTime;

    public CurrencyData(Currency currency) {
        this.requestId = currency.getRequestId();
        this.symbol = currency.getSymbol();
        this.bid = currency.getBid();
        this.ask = currency.getAsk();
        this.price = currency.getPrice();
        this.time = currency.getTimestamp();
        this.mongoTime = new Date(time * 1000);
    }

    //for mongo
    public CurrencyData() {
    }

    public Currency toCurrency() {
        return new Currency(requestId, symbol, bid, ask, price, time);
    }


    public String getRequestId() {
        return requestId;
    }

    public String getSymbol() {
        return symbol;
    }

    public BigDecimal getBid() {
        return bid;
    }

    public BigDecimal getAsk() {
        return ask;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public Long getTime() {
        return time;
    }
}
