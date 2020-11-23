package com.jinfeng.hive.util;

import com.esotericsoftware.minlog.Log;
import org.apache.hive.service.cli.thrift.*;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @package: com.jinfeng.hive.util
 * @author: wangjf
 * @date: 2020/10/21
 * @time: 3:18 下午
 * @email: jinfeng.wang@mobvista.com
 * @phone: 152-1062-7698
 */
public class QueryInstance {
    private static final String host = "52.86.251.176";
    private static final int port = 10000;
    private static final String username = "hadoop";
    private static final String passsword = "";
    private static TTransport transport;
    private static TCLIService.Client client;
    private TOperationState tOperationState = null;
    private Map<String, Object> resultMap = new HashMap<>();

    static {
        try {
            transport = QueryTool.getSocketInstance(host, port, username,
                    passsword);
            client = new TCLIService.Client(new TBinaryProtocol(transport));
            transport.open();
        } catch (TTransportException e) {
            //  Log.info(e.getMessage());
            Log.info("hive collection error!");
        }
    }

    public TOperationHandle submitQuery(String command) throws Exception {

        TOperationHandle tOperationHandle;
        TExecuteStatementResp resp;

        TSessionHandle sessHandle = QueryTool.openSession(client)
                .getSessionHandle();

        TExecuteStatementReq execReq = new TExecuteStatementReq(sessHandle,
                command);
        // 异步运行
        execReq.setRunAsync(true);
        // 执行sql
        resp = client.ExecuteStatement(execReq);

        // 获取执行的handle

        System.out.println(resp.getStatus().getErrorMessage());

        tOperationHandle = resp.getOperationHandle();

        if (tOperationHandle == null) {
            //语句执行异常时，会把异常信息放在resp.getStatus()中。
            throw new Exception(resp.getStatus().getErrorMessage());
        }
        return tOperationHandle;
    }

    public TOperationState getQueryHandleStatus(
            TOperationHandle tOperationHandle) throws Exception {

        if (tOperationHandle != null) {
            TGetOperationStatusReq statusReq = new TGetOperationStatusReq(
                    tOperationHandle);
            TGetOperationStatusResp statusResp = client
                    .GetOperationStatus(statusReq);

            tOperationState = statusResp.getOperationState();

        }
        return tOperationState;
    }

    public List<String> getColumns(TOperationHandle tOperationHandle)
            throws Throwable {
        TGetResultSetMetadataResp metadataResp;
        TGetResultSetMetadataReq metadataReq;
        TTableSchema tableSchema;
        metadataReq = new TGetResultSetMetadataReq(tOperationHandle);
        metadataResp = client.GetResultSetMetadata(metadataReq);
        List<TColumnDesc> columnDescs;
        List<String> columns = null;
        tableSchema = metadataResp.getSchema();
        if (tableSchema != null) {
            columnDescs = tableSchema.getColumns();
            columns = new ArrayList<>();
            for (TColumnDesc tColumnDesc : columnDescs) {
                columns.add(tColumnDesc.getColumnName());
            }
        }
        return columns;
    }

    /**
     * 获取执行结果 select语句
     */

    public List<Object> getResults(TOperationHandle tOperationHandle) throws Throwable {
        TFetchResultsReq fetchReq = new TFetchResultsReq();
        fetchReq.setOperationHandle(tOperationHandle);
        fetchReq.setMaxRows(1000);
        TFetchResultsResp re = client.FetchResults(fetchReq);
        List<TColumn> list = re.getResults().getColumns();
        List<Object> list_row = new ArrayList<>();
        for (TColumn field : list) {
            if (field.isSetStringVal()) {
                list_row.add(field.getStringVal().getValues());
            } else if (field.isSetDoubleVal()) {
                list_row.add(field.getDoubleVal().getValues());
            } else if (field.isSetI16Val()) {
                list_row.add(field.getI16Val().getValues());
            } else if (field.isSetI32Val()) {
                list_row.add(field.getI32Val().getValues());
            } else if (field.isSetI64Val()) {
                list_row.add(field.getI64Val().getValues());
            } else if (field.isSetBoolVal()) {
                list_row.add(field.getBoolVal().getValues());
            } else if (field.isSetByteVal()) {
                list_row.add(field.getByteVal().getValues());
            }
        }
        for (Object obj : list_row) {
            System.out.println(obj);
        }
        return list_row;
    }

    public void cancelQuery(TOperationHandle tOperationHandle) throws Throwable {
        if (tOperationState != TOperationState.FINISHED_STATE) {
            TCancelOperationReq cancelOperationReq = new TCancelOperationReq();
            cancelOperationReq.setOperationHandle(tOperationHandle);
            client.CancelOperation(cancelOperationReq);
        }
    }
}
