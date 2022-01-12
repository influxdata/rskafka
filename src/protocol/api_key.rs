//! ApiKey to tag request types.
//!
//! # References
//! - <https://kafka.apache.org/protocol#protocol_api_keys>

use super::primitives::Int16;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
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
            ApiKey::Produce => Self(0),
            ApiKey::Fetch => Self(1),
            ApiKey::ListOffsets => Self(2),
            ApiKey::Metadata => Self(3),
            ApiKey::LeaderAndIsr => Self(4),
            ApiKey::StopReplica => Self(5),
            ApiKey::UpdateMetadata => Self(6),
            ApiKey::ControlledShutdown => Self(7),
            ApiKey::OffsetCommit => Self(8),
            ApiKey::OffsetFetch => Self(9),
            ApiKey::FindCoordinator => Self(10),
            ApiKey::JoinGroup => Self(11),
            ApiKey::Heartbeat => Self(12),
            ApiKey::LeaveGroup => Self(13),
            ApiKey::SyncGroup => Self(14),
            ApiKey::DescribeGroups => Self(15),
            ApiKey::ListGroups => Self(16),
            ApiKey::SaslHandshake => Self(17),
            ApiKey::ApiVersions => Self(18),
            ApiKey::CreateTopics => Self(19),
            ApiKey::DeleteTopics => Self(20),
            ApiKey::DeleteRecords => Self(21),
            ApiKey::InitProducerId => Self(22),
            ApiKey::OffsetForLeaderEpoch => Self(23),
            ApiKey::AddPartitionsToTxn => Self(24),
            ApiKey::AddOffsetsToTxn => Self(25),
            ApiKey::EndTxn => Self(26),
            ApiKey::WriteTxnMarkers => Self(27),
            ApiKey::TxnOffsetCommit => Self(28),
            ApiKey::DescribeAcls => Self(29),
            ApiKey::CreateAcls => Self(30),
            ApiKey::DeleteAcls => Self(31),
            ApiKey::DescribeConfigs => Self(32),
            ApiKey::AlterConfigs => Self(33),
            ApiKey::AlterReplicaLogDirs => Self(34),
            ApiKey::DescribeLogDirs => Self(35),
            ApiKey::SaslAuthenticate => Self(36),
            ApiKey::CreatePartitions => Self(37),
            ApiKey::CreateDelegationToken => Self(38),
            ApiKey::RenewDelegationToken => Self(39),
            ApiKey::ExpireDelegationToken => Self(40),
            ApiKey::DescribeDelegationToken => Self(41),
            ApiKey::DeleteGroups => Self(42),
            ApiKey::ElectLeaders => Self(43),
            ApiKey::IncrementalAlterConfigs => Self(44),
            ApiKey::AlterPartitionReassignments => Self(45),
            ApiKey::ListPartitionReassignments => Self(46),
            ApiKey::OffsetDelete => Self(47),
            ApiKey::DescribeClientQuotas => Self(48),
            ApiKey::AlterClientQuotas => Self(49),
            ApiKey::DescribeUserScramCredentials => Self(50),
            ApiKey::AlterUserScramCredentials => Self(51),
            ApiKey::AlterIsr => Self(56),
            ApiKey::UpdateFeatures => Self(57),
            ApiKey::DescribeCluster => Self(60),
            ApiKey::DescribeProducers => Self(61),
            ApiKey::DescribeTransactions => Self(65),
            ApiKey::ListTransactions => Self(66),
            ApiKey::AllocateProducerIds => Self(67),
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
            let key = match key {
                // Ensure key is actually unknown
                ApiKey::Unknown(x) => ApiKey::from(x),
                _ => key,
            };

            let code = Int16::from(key);
            let key2 = ApiKey::from(code);
            assert_eq!(key, key2);
        }
    }
}
