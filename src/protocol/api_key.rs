//! ApiKey to tag request types.
//!
//! # References
//! - <https://kafka.apache.org/protocol#protocol_api_keys>

use super::primitives::Int16;

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub enum ApiKey {
    Produce,
    Fetch,
    ListOffsets,
    Metadata,
    LeaderAndIsr,
    StopReplica,
    UpdateMetadata,
    ControlledShutdown,
    OffsetCommit,
    OffsetFetch,
    FindCoordinator,
    JoinGroup,
    Heartbeat,
    LeaveGroup,
    SyncGroup,
    DescribeGroups,
    ListGroups,
    SaslHandshake,
    ApiVersions,
    CreateTopics,
    DeleteTopics,
    DeleteRecords,
    InitProducerId,
    OffsetForLeaderEpoch,
    AddPartitionsToTxn,
    AddOffsetsToTxn,
    EndTxn,
    WriteTxnMarkers,
    TxnOffsetCommit,
    DescribeAcls,
    CreateAcls,
    DeleteAcls,
    DescribeConfigs,
    AlterConfigs,
    AlterReplicaLogDirs,
    DescribeLogDirs,
    SaslAuthenticate,
    CreatePartitions,
    CreateDelegationToken,
    RenewDelegationToken,
    ExpireDelegationToken,
    DescribeDelegationToken,
    DeleteGroups,
    ElectLeaders,
    IncrementalAlterConfigs,
    AlterPartitionReassignments,
    ListPartitionReassignments,
    OffsetDelete,
    DescribeClientQuotas,
    AlterClientQuotas,
    DescribeUserScramCredentials,
    AlterUserScramCredentials,
    AlterIsr,
    UpdateFeatures,
    DescribeCluster,
    DescribeProducers,
    DescribeTransactions,
    ListTransactions,
    AllocateProducerIds,
    Unknown(Int16),
}

impl From<Int16> for ApiKey {
    fn from(key: Int16) -> Self {
        match key.0 {
            0 => Self::Produce,
            1 => Self::Fetch,
            2 => Self::ListOffsets,
            3 => Self::Metadata,
            4 => Self::LeaderAndIsr,
            5 => Self::StopReplica,
            6 => Self::UpdateMetadata,
            7 => Self::ControlledShutdown,
            8 => Self::OffsetCommit,
            9 => Self::OffsetFetch,
            10 => Self::FindCoordinator,
            11 => Self::JoinGroup,
            12 => Self::Heartbeat,
            13 => Self::LeaveGroup,
            14 => Self::SyncGroup,
            15 => Self::DescribeGroups,
            16 => Self::ListGroups,
            17 => Self::SaslHandshake,
            18 => Self::ApiVersions,
            19 => Self::CreateTopics,
            20 => Self::DeleteTopics,
            21 => Self::DeleteRecords,
            22 => Self::InitProducerId,
            23 => Self::OffsetForLeaderEpoch,
            24 => Self::AddPartitionsToTxn,
            25 => Self::AddOffsetsToTxn,
            26 => Self::EndTxn,
            27 => Self::WriteTxnMarkers,
            28 => Self::TxnOffsetCommit,
            29 => Self::DescribeAcls,
            30 => Self::CreateAcls,
            31 => Self::DeleteAcls,
            32 => Self::DescribeConfigs,
            33 => Self::AlterConfigs,
            34 => Self::AlterReplicaLogDirs,
            35 => Self::DescribeLogDirs,
            36 => Self::SaslAuthenticate,
            37 => Self::CreatePartitions,
            38 => Self::CreateDelegationToken,
            39 => Self::RenewDelegationToken,
            40 => Self::ExpireDelegationToken,
            41 => Self::DescribeDelegationToken,
            42 => Self::DeleteGroups,
            43 => Self::ElectLeaders,
            44 => Self::IncrementalAlterConfigs,
            45 => Self::AlterPartitionReassignments,
            46 => Self::ListPartitionReassignments,
            47 => Self::OffsetDelete,
            48 => Self::DescribeClientQuotas,
            49 => Self::AlterClientQuotas,
            50 => Self::DescribeUserScramCredentials,
            51 => Self::AlterUserScramCredentials,
            56 => Self::AlterIsr,
            57 => Self::UpdateFeatures,
            60 => Self::DescribeCluster,
            61 => Self::DescribeProducers,
            65 => Self::DescribeTransactions,
            66 => Self::ListTransactions,
            67 => Self::AllocateProducerIds,
            _ => Self::Unknown(key),
        }
    }
}

impl From<ApiKey> for Int16 {
    fn from(key: ApiKey) -> Self {
        match key {
            ApiKey::Produce => Int16(0),
            ApiKey::Fetch => Int16(1),
            ApiKey::ListOffsets => Int16(2),
            ApiKey::Metadata => Int16(3),
            ApiKey::LeaderAndIsr => Int16(4),
            ApiKey::StopReplica => Int16(5),
            ApiKey::UpdateMetadata => Int16(6),
            ApiKey::ControlledShutdown => Int16(7),
            ApiKey::OffsetCommit => Int16(8),
            ApiKey::OffsetFetch => Int16(9),
            ApiKey::FindCoordinator => Int16(10),
            ApiKey::JoinGroup => Int16(11),
            ApiKey::Heartbeat => Int16(12),
            ApiKey::LeaveGroup => Int16(13),
            ApiKey::SyncGroup => Int16(14),
            ApiKey::DescribeGroups => Int16(15),
            ApiKey::ListGroups => Int16(16),
            ApiKey::SaslHandshake => Int16(17),
            ApiKey::ApiVersions => Int16(18),
            ApiKey::CreateTopics => Int16(19),
            ApiKey::DeleteTopics => Int16(20),
            ApiKey::DeleteRecords => Int16(21),
            ApiKey::InitProducerId => Int16(22),
            ApiKey::OffsetForLeaderEpoch => Int16(23),
            ApiKey::AddPartitionsToTxn => Int16(24),
            ApiKey::AddOffsetsToTxn => Int16(25),
            ApiKey::EndTxn => Int16(26),
            ApiKey::WriteTxnMarkers => Int16(27),
            ApiKey::TxnOffsetCommit => Int16(28),
            ApiKey::DescribeAcls => Int16(29),
            ApiKey::CreateAcls => Int16(30),
            ApiKey::DeleteAcls => Int16(31),
            ApiKey::DescribeConfigs => Int16(32),
            ApiKey::AlterConfigs => Int16(33),
            ApiKey::AlterReplicaLogDirs => Int16(34),
            ApiKey::DescribeLogDirs => Int16(35),
            ApiKey::SaslAuthenticate => Int16(36),
            ApiKey::CreatePartitions => Int16(37),
            ApiKey::CreateDelegationToken => Int16(38),
            ApiKey::RenewDelegationToken => Int16(39),
            ApiKey::ExpireDelegationToken => Int16(40),
            ApiKey::DescribeDelegationToken => Int16(41),
            ApiKey::DeleteGroups => Int16(42),
            ApiKey::ElectLeaders => Int16(43),
            ApiKey::IncrementalAlterConfigs => Int16(44),
            ApiKey::AlterPartitionReassignments => Int16(45),
            ApiKey::ListPartitionReassignments => Int16(46),
            ApiKey::OffsetDelete => Int16(47),
            ApiKey::DescribeClientQuotas => Int16(48),
            ApiKey::AlterClientQuotas => Int16(49),
            ApiKey::DescribeUserScramCredentials => Int16(50),
            ApiKey::AlterUserScramCredentials => Int16(51),
            ApiKey::AlterIsr => Int16(56),
            ApiKey::UpdateFeatures => Int16(57),
            ApiKey::DescribeCluster => Int16(60),
            ApiKey::DescribeProducers => Int16(61),
            ApiKey::DescribeTransactions => Int16(65),
            ApiKey::ListTransactions => Int16(66),
            ApiKey::AllocateProducerIds => Int16(67),
            ApiKey::Unknown(code) => code,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn test_roundrip_int16(code: Int16) {
            let api_key = ApiKey::from(code);
            let code2 = Int16::from(api_key);
            assert_eq!(code, code2);
        }

        #[test]
        fn test_roundrip_api_key(key: ApiKey) {
            let code = Int16::from(key);
            let key2 = ApiKey::from(code);
            assert_eq!(key, key2);
        }
    }
}
