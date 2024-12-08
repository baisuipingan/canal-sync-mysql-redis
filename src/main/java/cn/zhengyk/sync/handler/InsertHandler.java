package cn.zhengyk.sync.handler;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * @author: Yakai Zheng（zhengyk@cloud-young.com）
 * @date: Created on 2018/12/18
 * @description: 监听到数据库插入操作后执行的逻辑
 * @version: 1.0
 */
@Slf4j
@Component
public class InsertHandler extends AbstractHandler {

    public InsertHandler() {
        this.eventType = EventType.INSERT;
    }

    @Autowired
    public void setNextHandler(DeleteHandler deleteHandler) {
        this.nextHandler = deleteHandler;
    }

    @Override
    public void handleRowChange(CanalEntry.Entry entry) {
        RowChange rowChange = null;
        String tableName = entry.getHeader().getTableName();
        try {
            rowChange = RowChange.parseFrom(entry.getStoreValue());
        } catch (InvalidProtocolBufferException e) {
            log.error("根据CanalEntry获取RowChange异常:", e);
            return;
        }
        rowChange.getRowDatasList().forEach(rowData -> {
            //每一行的每列数据  字段名->值
            List<Column> afterColumnsList = rowData.getAfterColumnsList();
            Map<String, String> map = super.columnsToMap(afterColumnsList);
            String id = map.get("id");
            String jsonStr = JSONObject.toJSONString(map);
            log.info("新增的数据：{}\r\n", jsonStr);
            redisUtil.setDefault(tableName + ":" + id, jsonStr);
        });
    }


}
