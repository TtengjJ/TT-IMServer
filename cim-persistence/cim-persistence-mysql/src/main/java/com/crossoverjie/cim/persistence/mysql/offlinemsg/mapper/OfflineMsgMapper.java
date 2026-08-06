package com.crossoverjie.cim.persistence.mysql.offlinemsg.mapper;

import com.crossoverjie.cim.persistence.api.pojo.OfflineMsg;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

@Mapper
public interface OfflineMsgMapper {

    int insert(OfflineMsg msg);

    int insertBatch(@Param("offlineMsgs") List<OfflineMsg> offlineMsgs);

    List<OfflineMsg> fetchOfflineMsgsWithCursor(@Param("receiveUserId") Long receiveUserId,
                                                @Param("limit") Integer limit);

    int updateStatus(@Param("receiveUserId") Long receiveUserId,
                     @Param("messageIds") List<Long> messageIds,
                     @Param("status") Integer status);

    int updateStatusByMessageId(@Param("messageId") Long messageId, @Param("status") Integer status);

    int prepareRetry(@Param("messageId") Long messageId, @Param("pendingStatus") Integer pendingStatus);

    OfflineMsg findByMessageId(@Param("messageId") Long messageId);

    List<OfflineMsg> listNeedRetry(@Param("maxRetry") int maxRetry, @Param("limit") int limit,
                                   @Param("pendingStatus") int pendingStatus,
                                   @Param("sentStatus") int sentStatus);

    List<Long> fetchOfflineMsgIdsWithCursor(@Param("receiveUserId") Long receiveUserId);
}
