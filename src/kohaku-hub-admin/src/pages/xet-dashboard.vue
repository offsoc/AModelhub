<script setup>
import { ref, computed, onMounted } from "vue";
import { useRouter } from "vue-router";
import AdminLayout from "@/components/AdminLayout.vue";
import StatsCard from "@/components/StatsCard.vue";
import ChartCard from "@/components/ChartCard.vue";
import { useAdminStore } from "@/stores/admin";
import {
  getXetStats,
  getTopXetRepos,
  getBlockDistribution,
  formatBytes,
} from "@/utils/api";
import { ElMessage } from "element-plus";

const router = useRouter();
const adminStore = useAdminStore();
const stats = ref(null);
const topRepos = ref([]);
const distribution = ref(null);
const loading = ref(false);

async function loadData() {
  if (!adminStore.token) {
    router.push("/login");
    return;
  }

  loading.value = true;
  try {
    const [statsData, topReposData, distData] = await Promise.all([
      getXetStats(adminStore.token),
      getTopXetRepos(adminStore.token, 10),
      getBlockDistribution(adminStore.token),
    ]);

    stats.value = statsData;
    topRepos.value = topReposData;
    distribution.value = distData;
  } catch (error) {
    console.error("Failed to load Xet stats:", error);
    ElMessage.error("Failed to load Xet dashboard data");
  } finally {
    loading.value = false;
  }
}

const distributionChart = computed(() => {
  if (!distribution.value) return null;

  const labels = ["<1MB", "1-4MB", "4-8MB", "8MB+"];
  const data = [
    distribution.value.under_1mb,
    distribution.value["1mb_4mb"],
    distribution.value["4mb_8mb"],
    distribution.value.over_8mb,
  ];

  return {
    labels,
    datasets: [
      {
        label: "Block Count",
        data,
        backgroundColor: [
          "rgba(64, 158, 255, 0.6)",
          "rgba(103, 194, 58, 0.6)",
          "rgba(230, 162, 60, 0.6)",
          "rgba(245, 108, 108, 0.6)",
        ],
      },
    ],
  };
});

onMounted(() => {
  loadData();
});
</script>

<template>
  <AdminLayout>
    <div class="page-container">
      <div class="flex items-center justify-between mb-6">
        <h1 class="text-3xl font-bold text-gray-900 dark:text-gray-100">
          Xet Metrics Dashboard
        </h1>
        <el-button type="primary" :icon="'Refresh'" @click="loadData">Refresh</el-button>
      </div>

      <div v-loading="loading" class="stats-grid">
        <StatsCard
          title="Deduplication Ratio"
          :value="`${stats?.metrics?.deduplication_ratio || 1.0}x`"
          subtitle="Storage Efficiency"
          icon="i-carbon-ChartAverage"
          color="green"
        />
        <StatsCard
          title="Storage Savings"
          :value="formatBytes(stats?.metrics?.savings_bytes || 0)"
          subtitle="Space Saved via CAS"
          icon="i-carbon-Enterprise"
          color="blue"
        />
        <StatsCard
          title="Total Blocks"
          :value="stats?.blocks?.count || 0"
          :subtitle="`Logical Size: ${formatBytes(stats?.blocks?.logical_size_bytes || 0)}`"
          icon="i-carbon-Cube"
          color="purple"
        />
        <StatsCard
          title="Total XORBs"
          :value="stats?.xorbs?.count || 0"
          :subtitle="`Physical Size: ${formatBytes(stats?.xorbs?.physical_size_bytes || 0)}`"
          icon="i-carbon-Archive"
          color="orange"
        />
      </div>

      <div class="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
        <!-- Block Distribution Chart -->
        <ChartCard
          v-if="distributionChart"
          title="Block Size Distribution"
          type="bar"
          :labels="distributionChart.labels"
          :datasets="distributionChart.datasets"
          :height="300"
        />

        <!-- Top Repositories by Xet Usage -->
        <el-card>
          <template #header>
            <div class="flex items-center justify-between">
              <span class="font-bold">Top Repositories (by Xet Usage)</span>
            </div>
          </template>
          <el-table :data="topRepos" stripe size="small">
            <el-table-column label="Repository" min-width="200">
              <template #default="{ row }">
                <div class="flex items-center gap-2">
                  <el-tag size="small" :type="row.repo_type === 'dataset' ? 'success' : 'primary'">
                    {{ row.repo_type }}
                  </el-tag>
                  <span class="font-mono text-xs">{{ row.repo_full_id }}</span>
                </div>
              </template>
            </el-table-column>
            <el-table-column label="Logical Size" width="120" align="right">
              <template #default="{ row }">
                {{ formatBytes(row.logical_size_bytes) }}
              </template>
            </el-table-column>
            <el-table-column prop="block_count" label="Blocks" width="100" align="right" />
          </el-table>
        </el-card>
      </div>

      <!-- Xet Protocol Insights -->
      <el-card class="mb-6">
        <template #header>
          <span class="font-bold">Xet Protocol Insights</span>
        </template>
        <div class="grid grid-cols-1 md:grid-cols-3 gap-6">
          <div class="insight-item">
            <h4 class="text-sm font-semibold text-gray-500 mb-1">Metadata Shards</h4>
            <p class="text-2xl font-bold">{{ stats?.shards?.count || 0 }}</p>
            <p class="text-xs text-gray-400 mt-1">Total Shard Size: {{ formatBytes(stats?.shards?.size_bytes || 0) }}</p>
          </div>
          <div class="insight-item">
            <h4 class="text-sm font-semibold text-gray-500 mb-1">Compaction Status</h4>
            <el-tag type="success" effect="dark" size="small">Healthy</el-tag>
            <p class="text-xs text-gray-400 mt-1">Background compaction worker active</p>
          </div>
          <div class="insight-item">
            <h4 class="text-sm font-semibold text-gray-500 mb-1">Bloom Filter</h4>
            <el-tag type="primary" effect="dark" size="small">Active</el-tag>
            <p class="text-xs text-gray-400 mt-1">Redis Filter optimized for O(1) dedupe</p>
          </div>
        </div>
      </el-card>
    </div>
  </AdminLayout>
</template>

<style scoped>
.page-container {
  padding: 24px;
}
.stats-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
  gap: 24px;
  margin-bottom: 32px;
}
.insight-item {
  padding: 16px;
  background: var(--bg-hover);
  border-radius: 8px;
  border: 1px solid var(--border-light);
}
</style>
