package org.example.service;

import lombok.extern.log4j.Log4j2;
import org.apache.avro.Schema;
import org.example.model.PipelineSchema;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.util.Map;

/**
 * Returns a PipelineSchema (DuckDB column types) for each known pipeline.
 *
 * Avro schemas are identical to those in kafka-s3-flink2's SchemaService.
 * Timestamp fields mirror TIMESTAMP_FIELDS_MAP from the same class:
 * those string fields are emitted as TIMESTAMP columns in DuckDB/Parquet.
 *
 * Throws IllegalArgumentException for unknown pipeline names.
 */
@Log4j2
@Service
public class SchemaService implements Serializable {

    public PipelineSchema getSchema(String pipelineName) {
        Schema avro = AVRO_SCHEMAS.get(pipelineName);
        if (avro == null) {
            throw new IllegalArgumentException("No schema registered for pipeline: " + pipelineName);
        }
        return AvroToDuckDbConverter.convert(pipelineName, avro);
    }

    public Schema getFinalSchemaByPipelineName(String pipelineName) {
        Schema schema = AVRO_SCHEMAS.get(pipelineName);
        if (schema == null) {
            throw new IllegalArgumentException("No schema registered for pipeline: " + pipelineName);
        }
        return schema;
    }

    // -------------------------------------------------------------------------
    // Avro schemas — identical to kafka-s3-flink2 SchemaService.FINAL_SCHEMA_MAP
    // -------------------------------------------------------------------------

    private static final Map<String, Schema> AVRO_SCHEMAS = Map.of(

        "etl_ecommerce_orders", parse(
            "{\"type\":\"record\",\"name\":\"OrderEnvelope\",\"namespace\":\"commerce.orders\","
            + "\"fields\":["
            + "{\"name\":\"pipelineName\",\"type\":[\"null\",\"string\"]},"
            + "{\"name\":\"orderId\",\"type\":\"string\"},"
            + "{\"name\":\"eventType\",\"type\":\"string\"},"
            + "{\"name\":\"occurredAt\",\"type\":\"string\"},"
            + "{\"name\":\"customerId\",\"type\":\"string\"},"
            + "{\"name\":\"salesChannel\",\"type\":\"string\"},"
            + "{\"name\":\"currency\",\"type\":\"string\"},"
            + "{\"name\":\"pricing\",\"type\":{\"type\":\"record\",\"name\":\"Pricing\","
            +   "\"fields\":["
            +     "{\"name\":\"subtotal\",\"type\":\"double\"},"
            +     "{\"name\":\"tax\",\"type\":\"double\"},"
            +     "{\"name\":\"discounts\",\"type\":\"double\"},"
            +     "{\"name\":\"totalAmount\",\"type\":\"double\"}"
            +   "]}},"
            + "{\"name\":\"items\",\"type\":{\"type\":\"array\",\"items\":{"
            +   "\"type\":\"record\",\"name\":\"OrderItem\","
            +   "\"fields\":["
            +     "{\"name\":\"sku\",\"type\":\"string\"},"
            +     "{\"name\":\"quantity\",\"type\":\"int\"},"
            +     "{\"name\":\"unitPrice\",\"type\":\"double\"},"
            +     "{\"name\":\"isOnSale\",\"type\":[\"null\",\"boolean\"],\"default\":null},"
            +     "{\"name\":\"warehouseId\",\"type\":[\"null\",\"long\"],\"default\":null}"
            +   "]}}},"
            + "{\"name\":\"shipping\",\"type\":{\"type\":\"record\",\"name\":\"Shipping\","
            +   "\"fields\":["
            +     "{\"name\":\"city\",\"type\":\"string\"},"
            +     "{\"name\":\"method\",\"type\":\"string\"},"
            +     "{\"name\":\"promisedDate\",\"type\":\"string\"}"
            +   "]}},"
            + "{\"name\":\"tagIds\",\"type\":[\"null\",{\"type\":\"array\",\"items\":\"int\"}],\"default\":null},"
            + "{\"name\":\"discountRates\",\"type\":[\"null\",{\"type\":\"array\",\"items\":\"double\"}],\"default\":null},"
            + "{\"name\":\"loyaltyPoints\",\"type\":[\"null\",{\"type\":\"array\",\"items\":\"long\"}],\"default\":null},"
            + "{\"name\":\"promoFlags\",\"type\":[\"null\",{\"type\":\"array\",\"items\":\"boolean\"}],\"default\":null}"
            + "]}"
        ),

        "etl_payments_clearing", parse(
            "{\"type\":\"record\",\"name\":\"SettlementBatch\",\"namespace\":\"payments.settlement\","
            + "\"fields\":["
            + "{\"name\":\"pipelineName\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"settlementId\",\"type\":\"string\"},"
            + "{\"name\":\"transactionId\",\"type\":\"string\"},"
            + "{\"name\":\"merchantId\",\"type\":\"string\"},"
            + "{\"name\":\"processor\",\"type\":\"string\"},"
            + "{\"name\":\"currency\",\"type\":\"string\"},"
            + "{\"name\":\"amount\",\"type\":\"double\"},"
            + "{\"name\":\"fees\",\"type\":{\"type\":\"record\",\"name\":\"FeeBreakdown\","
            +   "\"fields\":["
            +     "{\"name\":\"interchange\",\"type\":\"double\"},"
            +     "{\"name\":\"processing\",\"type\":\"double\"}"
            +   "]}}"
            + "]}"
        ),

        "etl_customer_journeys", parse(
            "{\"type\":\"record\",\"name\":\"JourneyEvent\",\"namespace\":\"marketing.journey\","
            + "\"fields\":["
            + "{\"name\":\"pipelineName\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"sessionId\",\"type\":\"string\"},"
            + "{\"name\":\"customerId\",\"type\":\"string\"},"
            + "{\"name\":\"channel\",\"type\":\"string\"},"
            + "{\"name\":\"device\",\"type\":\"string\"},"
            + "{\"name\":\"geo\",\"type\":\"string\"},"
            + "{\"name\":\"metrics\",\"type\":{\"type\":\"record\",\"name\":\"JourneyMetrics\","
            +   "\"fields\":["
            +     "{\"name\":\"durationMs\",\"type\":\"long\"},"
            +     "{\"name\":\"pagesViewed\",\"type\":\"int\"},"
            +     "{\"name\":\"conversions\",\"type\":\"int\"}"
            +   "]}}"
            + "]}"
        ),

        "etl_iot_sensor_batches", parse(
            "{\"type\":\"record\",\"name\":\"SensorEnvelope\",\"namespace\":\"iot.telemetry\","
            + "\"fields\":["
            + "{\"name\":\"pipelineName\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"deviceId\",\"type\":\"string\"},"
            + "{\"name\":\"siteId\",\"type\":\"string\"},"
            + "{\"name\":\"firmwareVersion\",\"type\":\"string\"},"
            + "{\"name\":\"metrics\",\"type\":{\"type\":\"array\",\"items\":{"
            +   "\"type\":\"record\",\"name\":\"SensorMetric\","
            +   "\"fields\":["
            +     "{\"name\":\"name\",\"type\":\"string\"},"
            +     "{\"name\":\"value\",\"type\":\"double\"},"
            +     "{\"name\":\"unit\",\"type\":\"string\"}"
            +   "]}}}"
            + "]}"
        ),

        "etl_logistics_shipments", parse(
            "{\"type\":\"record\",\"name\":\"ShipmentEvent\",\"namespace\":\"logistics.shipment\","
            + "\"fields\":["
            + "{\"name\":\"pipelineName\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"shipmentId\",\"type\":\"string\"},"
            + "{\"name\":\"carrier\",\"type\":\"string\"},"
            + "{\"name\":\"status\",\"type\":\"string\"},"
            + "{\"name\":\"milestones\",\"type\":{\"type\":\"array\",\"items\":{"
            +   "\"type\":\"record\",\"name\":\"ShipmentMilestone\","
            +   "\"fields\":["
            +     "{\"name\":\"name\",\"type\":\"string\"},"
            +     "{\"name\":\"location\",\"type\":\"string\"},"
            +     "{\"name\":\"timestamp\",\"type\":\"string\"}"
            +   "]}}}"
            + "]}"
        ),

        "etl_marketing_campaigns", parse(
            "{\"type\":\"record\",\"name\":\"CampaignResponse\",\"namespace\":\"marketing.campaign\","
            + "\"fields\":["
            + "{\"name\":\"pipelineName\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"campaignId\",\"type\":\"string\"},"
            + "{\"name\":\"messageId\",\"type\":\"string\"},"
            + "{\"name\":\"recipientId\",\"type\":\"string\"},"
            + "{\"name\":\"channel\",\"type\":\"string\"},"
            + "{\"name\":\"events\",\"type\":{\"type\":\"array\",\"items\":{"
            +   "\"type\":\"record\",\"name\":\"CampaignEvent\","
            +   "\"fields\":["
            +     "{\"name\":\"type\",\"type\":\"string\"},"
            +     "{\"name\":\"timestamp\",\"type\":\"string\"}"
            +   "]}}}"
            + "]}"
        ),

        "etl_fraud_alerts", parse(
            "{\"type\":\"record\",\"name\":\"FraudAlert\",\"namespace\":\"risk.alerts\","
            + "\"fields\":["
            + "{\"name\":\"pipelineName\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"alertId\",\"type\":\"string\"},"
            + "{\"name\":\"transactionId\",\"type\":\"string\"},"
            + "{\"name\":\"score\",\"type\":\"double\"},"
            + "{\"name\":\"reasonCodes\",\"type\":{\"type\":\"array\",\"items\":\"string\"}}"
            + "]}"
        ),

        "etl_support_tickets", parse(
            "{\"type\":\"record\",\"name\":\"SupportTicket\",\"namespace\":\"support.tickets\","
            + "\"fields\":["
            + "{\"name\":\"pipelineName\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"ticketId\",\"type\":\"string\"},"
            + "{\"name\":\"customerId\",\"type\":\"string\"},"
            + "{\"name\":\"channel\",\"type\":\"string\"},"
            + "{\"name\":\"priority\",\"type\":\"string\"},"
            + "{\"name\":\"openedAt\",\"type\":\"string\"}"
            + "]}"
        ),

        "etl_inventory_balances", parse(
            "{\"type\":\"record\",\"name\":\"InventorySnapshot\",\"namespace\":\"operations.inventory\","
            + "\"fields\":["
            + "{\"name\":\"pipelineName\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"sku\",\"type\":\"string\"},"
            + "{\"name\":\"warehouse\",\"type\":\"string\"},"
            + "{\"name\":\"onHand\",\"type\":\"int\"},"
            + "{\"name\":\"reserved\",\"type\":\"int\"},"
            + "{\"name\":\"availableToPromise\",\"type\":\"int\"}"
            + "]}"
        ),

        "etl_billing_invoices", parse(
            "{\"type\":\"record\",\"name\":\"Invoice\",\"namespace\":\"finance.billing\","
            + "\"fields\":["
            + "{\"name\":\"pipelineName\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"invoiceId\",\"type\":\"string\"},"
            + "{\"name\":\"accountId\",\"type\":\"string\"},"
            + "{\"name\":\"issuedAt\",\"type\":\"string\"},"
            + "{\"name\":\"billingPeriod\",\"type\":\"string\"},"
            + "{\"name\":\"currency\",\"type\":\"string\"},"
            + "{\"name\":\"amountDue\",\"type\":\"double\"}"
            + "]}"
        )
    );

    private static Schema parse(String json) {
        return new Schema.Parser().parse(json);
    }
}
