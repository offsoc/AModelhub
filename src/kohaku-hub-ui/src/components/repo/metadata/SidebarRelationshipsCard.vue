<script setup>
import { computed, ref } from "vue";

const props = defineProps({
  namespace: {
    type: String,
    required: true,
  },
  namespaceLink: {
    type: String,
    required: true,
  },
  metadata: {
    type: Object,
    default: () => ({}),
  },
  repoType: {
    type: String,
    required: true,
  },
});

const INITIAL_SHOW = 2;
const baseModelsExpanded = ref(false);
const datasetsExpanded = ref(false);

const pipelineTag = computed(() => props.metadata.pipeline_tag);
const libraryName = computed(() => props.metadata.library_name);
const languages = computed(() => {
  if (!props.metadata.language) return [];
  return Array.isArray(props.metadata.language)
    ? props.metadata.language
    : [props.metadata.language];
});
const sizeCategories = computed(() => {
  if (!props.metadata.size_categories) return [];
  return Array.isArray(props.metadata.size_categories)
    ? props.metadata.size_categories
    : [props.metadata.size_categories];
});
const tags = computed(() => {
  if (!props.metadata.tags) return [];
  return Array.isArray(props.metadata.tags)
    ? props.metadata.tags
    : [props.metadata.tags];
});

// Task icon mapping
const taskIconMap = {
  'text-classification': 'i-carbon-text-annotation',
  'token-classification': 'i-carbon-text-bold',
  'question-answering': 'i-carbon-help',
  'summarization': 'i-carbon-document-summary',
  'translation': 'i-carbon-language',
  'text-generation': 'i-carbon-pen',
  'fill-mask': 'i-carbon-text-fill',
  'image-classification': 'i-carbon-image-search',
  'object-detection': 'i-carbon-data-view',
  'image-segmentation': 'i-carbon-cut',
  'image-to-text': 'i-carbon-image-reference',
  'text-to-image': 'i-carbon-generate-pdf',
  'audio-classification': 'i-carbon-microphone',
  'automatic-speech-recognition': 'i-carbon-speech-to-text',
  'text-to-speech': 'i-carbon-text-to-speech',
  'audio-to-audio': 'i-carbon-music',
  'tabular-classification': 'i-carbon-data-table',
  'tabular-regression': 'i-carbon-chart-line',
  'zero-shot-classification': 'i-carbon-watson',
};

const getTaskIcon = computed(() => {
  return taskIconMap[pipelineTag.value] || 'i-carbon-idea';
});

const getTaskColor = computed(() => {
  if (!pipelineTag.value) return 'purple';
  if (pipelineTag.value.includes('text')) return 'blue';
  if (pipelineTag.value.includes('image')) return 'green';
  if (pipelineTag.value.includes('audio')) return 'orange';
  if (pipelineTag.value.includes('tabular')) return 'purple';
  return 'purple';
});

const libraryIcon = computed(() => {
  const iconMap = {
    'datasets': 'i-carbon-data-set',
    'pandas': 'i-carbon-pandas',
    'polars': 'i-carbon-chart-polar',
    'duckdb': 'i-carbon-data-base',
    'sklearn': 'i-carbon-machine-learning',
    'pytorch': 'i-carbon-model',
    'tensorflow': 'i-carbon-machine-learning-model',
    'jax': 'i-carbon-chemistry',
  };
  return iconMap[libraryName.value?.toLowerCase()] || 'i-carbon-tool-box';
});

const allBaseModels = computed(() => {
  if (!props.metadata.base_model) return [];
  return Array.isArray(props.metadata.base_model)
    ? props.metadata.base_model
    : [props.metadata.base_model];
});

const visibleBaseModels = computed(() => {
  if (baseModelsExpanded.value || allBaseModels.value.length <= INITIAL_SHOW) {
    return allBaseModels.value;
  }
  return allBaseModels.value.slice(0, INITIAL_SHOW);
});

const hasMoreBaseModels = computed(() => {
  return allBaseModels.value.length > INITIAL_SHOW;
});

const remainingBaseModels = computed(() => {
  return allBaseModels.value.length - INITIAL_SHOW;
});

const allDatasets = computed(() => {
  if (!props.metadata.datasets) return [];
  return Array.isArray(props.metadata.datasets)
    ? props.metadata.datasets
    : [props.metadata.datasets];
});

const visibleDatasets = computed(() => {
  if (datasetsExpanded.value || allDatasets.value.length <= INITIAL_SHOW) {
    return allDatasets.value;
  }
  return allDatasets.value.slice(0, INITIAL_SHOW);
});

const hasMoreDatasets = computed(() => {
  return allDatasets.value.length > INITIAL_SHOW;
});

const remainingDatasets = computed(() => {
  return allDatasets.value.length - INITIAL_SHOW;
});

const hasContent = computed(() => {
  // Always show card since we always have author
  return true;
});
</script>

<template>
  <div v-if="hasContent" class="card">
    <h3 class="font-semibold mb-3 text-gray-900 dark:text-white">
      Relationships
    </h3>
    <div class="space-y-3 text-sm">
      <!-- Author -->
      <div>
        <div class="text-xs text-gray-600 dark:text-gray-400 mb-1">Author:</div>
        <RouterLink
          :to="namespaceLink"
          class="flex items-center gap-2 p-1.5 hover:bg-gray-100 dark:hover:bg-gray-700 rounded transition-colors"
        >
          <div
            class="i-carbon-user-avatar text-base text-gray-400 dark:text-gray-500 flex-shrink-0"
          />
          <span
            class="text-blue-600 dark:text-blue-400 hover:underline truncate"
            >{{ namespace }}</span
          >
        </RouterLink>
      </div>

      <!-- Task -->
      <div v-if="pipelineTag">
        <div class="text-xs text-gray-600 dark:text-gray-400 mb-1">Task:</div>
        <div :class="[
          'flex items-center gap-2 p-1.5 rounded border',
          `bg-${getTaskColor}-50 dark:bg-${getTaskColor}-900/20 border-${getTaskColor}-200 dark:border-${getTaskColor}-800`
        ]">
            <div :class="[getTaskIcon, `text-${getTaskColor}-600 dark:text-${getTaskColor}-400`, 'text-base flex-shrink-0']"/>
            <span :class="[
              'font-medium px-1.5 py-0.5 rounded text-xs',
              `bg-${getTaskColor}-100 dark:bg-${getTaskColor}-900/40 text-${getTaskColor}-700 dark:text-${getTaskColor}-300`
            ]" :title="`Pipeline task: ${pipelineTag}`">{{ pipelineTag }}</span>
        </div>
      </div>

      <!-- Library -->
      <div v-if="libraryName">
        <div class="text-xs text-gray-600 dark:text-gray-400 mb-1">Library:</div>
         <div class="flex items-center justify-between gap-2 p-1.5 bg-orange-50 dark:bg-orange-900/20 rounded border border-orange-200 dark:border-orange-800">
            <div class="flex items-center gap-2">
              <div :class="libraryIcon" class="text-base text-orange-600 dark:text-orange-400 flex-shrink-0"/>
              <span class="font-medium text-sm">{{ libraryName }}</span>
            </div>
            <span class="inline-flex items-center gap-1 px-1.5 py-0.5 bg-green-100 dark:bg-green-900/40 text-green-700 dark:text-green-300 rounded text-xs">
              <div class="i-carbon-checkmark text-xs" />
              Supported
            </span>
        </div>
      </div>

      <!-- Languages -->
      <div v-if="languages.length > 0">
         <div class="text-xs text-gray-600 dark:text-gray-400 mb-1">Languages:</div>
         <div class="flex flex-wrap gap-1">
            <span v-for="lang in languages" :key="lang" class="inline-flex items-center gap-1 px-2 py-0.5 bg-blue-50 dark:bg-blue-900/20 text-blue-700 dark:text-blue-300 rounded text-xs border border-blue-200 dark:border-blue-800">
                <div class="i-carbon-language text-xs" />
                {{ lang }}
            </span>
         </div>
      </div>

      <!-- Size Categories -->
      <div v-if="sizeCategories.length > 0">
         <div class="text-xs text-gray-600 dark:text-gray-400 mb-1">Size:</div>
         <div class="flex flex-wrap gap-1">
            <span v-for="size in sizeCategories" :key="size" class="inline-flex items-center gap-1 px-2 py-0.5 bg-purple-50 dark:bg-purple-900/20 text-purple-700 dark:text-purple-300 rounded text-xs border border-purple-200 dark:border-purple-800">
                <div class="i-carbon-data-vis-1 text-xs" />
                {{ size }}
            </span>
         </div>
      </div>

      <!-- Tags -->
      <div v-if="tags.length > 0">
         <div class="text-xs text-gray-600 dark:text-gray-400 mb-1">Tags:</div>
         <div class="flex flex-wrap gap-1">
            <span v-for="tag in tags" :key="tag" class="px-2 py-0.5 bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-300 rounded text-xs border border-gray-200 dark:border-gray-600">
                {{ tag }}
            </span>
         </div>
      </div>

      <!-- Base Models -->
      <div v-if="allBaseModels.length > 0">
        <div class="text-xs text-gray-600 dark:text-gray-400 mb-1">
          Base Model:
        </div>
        <div class="space-y-1">
          <RouterLink
            v-for="model in visibleBaseModels"
            :key="model"
            :to="`/models/${model}`"
            class="flex items-center gap-2 p-1.5 hover:bg-gray-100 dark:hover:bg-gray-700 rounded transition-colors group"
          >
            <div
              class="i-carbon-model text-base text-blue-500 dark:text-blue-400 flex-shrink-0"
            />
            <span
              class="text-xs text-blue-600 dark:text-blue-400 group-hover:underline truncate"
            >
              {{ model }}
            </span>
          </RouterLink>
          <button
            v-if="hasMoreBaseModels"
            @click="baseModelsExpanded = !baseModelsExpanded"
            class="w-full py-1 text-xs text-gray-600 dark:text-gray-400 hover:text-blue-600 dark:hover:text-blue-400 hover:bg-gray-100 dark:hover:bg-gray-700 rounded transition-colors flex items-center justify-center gap-1"
          >
            <span v-if="!baseModelsExpanded"
              >Show {{ remainingBaseModels }} more</span
            >
            <span v-else>Show less</span>
            <div
              :class="
                baseModelsExpanded
                  ? 'i-carbon-chevron-up'
                  : 'i-carbon-chevron-down'
              "
              class="text-xs"
            />
          </button>
        </div>
      </div>

      <!-- Datasets -->
      <div v-if="allDatasets.length > 0">
        <div class="text-xs text-gray-600 dark:text-gray-400 mb-1">
          Datasets:
        </div>
        <div class="space-y-1">
          <RouterLink
            v-for="dataset in visibleDatasets"
            :key="dataset"
            :to="`/datasets/${dataset}`"
            class="flex items-center gap-2 p-1.5 hover:bg-gray-100 dark:hover:bg-gray-700 rounded transition-colors group"
          >
            <div
              class="i-carbon-data-set text-base text-green-500 dark:text-green-400 flex-shrink-0"
            />
            <span
              class="text-xs text-blue-600 dark:text-blue-400 group-hover:underline truncate"
            >
              {{ dataset }}
            </span>
          </RouterLink>
          <button
            v-if="hasMoreDatasets"
            @click="datasetsExpanded = !datasetsExpanded"
            class="w-full py-1 text-xs text-gray-600 dark:text-gray-400 hover:text-blue-600 dark:hover:text-blue-400 hover:bg-gray-100 dark:hover:bg-gray-700 rounded transition-colors flex items-center justify-center gap-1"
          >
            <span v-if="!datasetsExpanded"
              >Show {{ remainingDatasets }} more</span
            >
            <span v-else>Show less</span>
            <div
              :class="
                datasetsExpanded
                  ? 'i-carbon-chevron-up'
                  : 'i-carbon-chevron-down'
              "
              class="text-xs"
            />
          </button>
        </div>
      </div>
    </div>
  </div>
</template>
