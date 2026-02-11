<template>
  <div>
    <!-- Header con filtros -->
    <div class="bg-white rounded-lg shadow mb-6">
      <div class="px-6 py-4 border-b border-gray-200">
        <div class="flex flex-col sm:flex-row sm:items-center sm:justify-between">
          <div>
            <h2 class="text-lg font-medium text-gray-900">
              {{ isActiveMode ? 'Procesos Activos' : 'Historial de Ejecuciones' }}
            </h2>
            <p v-if="isHistoryMode" class="text-xs text-gray-500 mt-1">
              Mostrando solo ejecuciones finalizadas (no incluye procesos activos)
            </p>
            <p v-if="isActiveMode" class="text-xs text-gray-500 mt-1">
              Mostrando solo procesos en ejecución
            </p>
          </div>
          <div class="mt-4 sm:mt-0 flex items-center space-x-3">
            <!-- Filtro de estado -->
            <select
              v-model="filter"
              @change="onFilterChange"
              class="px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500"
            >
              <option value="all">Todas</option>
              <option value="history">Historial (finalizadas)</option>
              <option value="active">Activas</option>
              <option value="completed">Completadas</option>
              <option value="failed">Fallidas</option>
              <option value="cancelled">Canceladas</option>
            </select>

            <!-- Filtro de tipo -->
            <select
              v-model="typeFilter"
              @change="loadExecutions"
              class="px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500"
            >
              <option value="all">Todos los tipos</option>
              <option value="seeding">Seeding</option>
              <option value="sync">Sincronización</option>
            </select>

            <!-- Botón refresh -->
            <button
              @click="loadExecutions"
              class="px-3 py-2 bg-blue-600 text-white rounded-md text-sm hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              Actualizar
            </button>
          </div>
        </div>
      </div>

      <!-- Stats rápidas -->
      <div class="grid grid-cols-2 sm:grid-cols-4 gap-4 px-6 py-4 bg-gray-50">
        <div>
          <div class="text-xs text-gray-500">Total</div>
          <div class="mt-1 text-xl font-semibold text-gray-900">{{ totalCount }}</div>
        </div>
        <div>
          <div class="text-xs text-gray-500">Activas</div>
          <div class="mt-1 text-xl font-semibold text-green-600">{{ activeCount }}</div>
        </div>
        <div>
          <div class="text-xs text-gray-500">Completadas</div>
          <div class="mt-1 text-xl font-semibold text-blue-600">{{ completedCount }}</div>
        </div>
        <div>
          <div class="text-xs text-gray-500">Fallidas</div>
          <div class="mt-1 text-xl font-semibold text-red-600">{{ failedCount }}</div>
        </div>
      </div>
    </div>

    <!-- Lista de ejecuciones -->
    <div class="bg-white rounded-lg shadow">
      <!-- Loading state -->
      <div v-if="loading" class="p-12 text-center">
        <div class="inline-block animate-spin rounded-full h-8 w-8 border-4 border-blue-500 border-t-transparent"></div>
        <p class="mt-4 text-gray-600">Cargando ejecuciones...</p>
      </div>

      <!-- Empty state -->
      <div v-else-if="executions.length === 0" class="p-12 text-center">
        <svg class="mx-auto h-12 w-12 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
        </svg>
        <p class="mt-4 text-gray-600">No hay ejecuciones que coincidan con el filtro</p>
      </div>

      <!-- Tabla de ejecuciones -->
      <div v-else class="overflow-x-auto">
        <table class="min-w-full divide-y divide-gray-200">
          <thead class="bg-gray-50">
            <tr>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                ID / Tipo
              </th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Entidad
              </th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Estado
              </th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Progreso
              </th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Registros
              </th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Fecha Inicio
              </th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Duración
              </th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Acciones
              </th>
            </tr>
          </thead>
          <tbody class="bg-white divide-y divide-gray-200">
            <tr
              v-for="execution in executions"
              :key="execution.execution_id"
              class="hover:bg-gray-50 transition-colors"
            >
              <!-- ID / Tipo -->
              <td class="px-6 py-4 whitespace-nowrap">
                <div class="text-sm font-medium text-gray-900">
                  {{ execution.execution_id.substring(0, 8) }}
                </div>
                <div class="text-xs text-gray-500">{{ execution.execution_type || 'N/A' }}</div>
              </td>

              <!-- Entidad -->
              <td class="px-6 py-4 whitespace-nowrap">
                <div class="text-sm text-gray-900">{{ execution.entity || 'N/A' }}</div>
                <div v-if="execution.year" class="text-xs text-gray-500">Año: {{ execution.year }}</div>
              </td>

              <!-- Estado -->
              <td class="px-6 py-4 whitespace-nowrap">
                <span :class="getStatusClass(execution.status)" class="px-2 py-1 text-xs font-medium rounded-full">
                  {{ getStatusLabel(execution.status) }}
                </span>
              </td>

              <!-- Progreso -->
              <td class="px-6 py-4 whitespace-nowrap">
                <div class="w-24">
                  <div class="flex items-center">
                    <div class="flex-1">
                      <div class="w-full bg-gray-200 rounded-full h-2">
                        <div
                          :class="getProgressColor(execution.status)"
                          class="h-2 rounded-full transition-all"
                          :style="{ width: `${execution.progress || 0}%` }"
                        ></div>
                      </div>
                    </div>
                    <span class="ml-2 text-xs text-gray-600">{{ execution.progress || 0 }}%</span>
                  </div>
                </div>
              </td>

              <!-- Registros -->
              <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                <div>{{ formatNumber(execution.records_processed || 0) }}</div>
                <div v-if="execution.records_failed > 0" class="text-xs text-red-600">
                  {{ execution.records_failed }} errores
                </div>
              </td>

              <!-- Fecha Inicio -->
              <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                {{ formatDateTime(execution.started_at) }}
              </td>

              <!-- Duración -->
              <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                {{ formatDuration(execution.elapsed_time) }}
              </td>

              <!-- Acciones -->
              <td class="px-6 py-4 whitespace-nowrap text-sm font-medium">
                <button
                  @click="viewDetails(execution)"
                  class="text-blue-600 hover:text-blue-900 mr-3"
                  title="Ver detalles"
                >
                  Ver
                </button>
                <button
                  v-if="execution.status === 'running'"
                  @click="cancelExecution(execution.execution_id)"
                  class="text-red-600 hover:text-red-900"
                  title="Cancelar"
                >
                  Cancelar
                </button>
              </td>
            </tr>
          </tbody>
        </table>
      </div>

      <!-- Paginación -->
      <div v-if="executions.length > 0" class="px-6 py-4 border-t border-gray-200 flex items-center justify-between">
        <div class="text-sm text-gray-700">
          Mostrando {{ executions.length }} de {{ totalCount }} ejecuciones
        </div>
        <div class="flex space-x-2">
          <button
            @click="previousPage"
            :disabled="currentPage === 1"
            class="px-3 py-1 border border-gray-300 rounded-md text-sm disabled:opacity-50 disabled:cursor-not-allowed hover:bg-gray-50"
          >
            Anterior
          </button>
          <button
            @click="nextPage"
            :disabled="executions.length < pageSize"
            class="px-3 py-1 border border-gray-300 rounded-md text-sm disabled:opacity-50 disabled:cursor-not-allowed hover:bg-gray-50"
          >
            Siguiente
          </button>
        </div>
      </div>
    </div>

    <!-- Modal de detalles -->
    <div
      v-if="selectedExecution"
      @click="selectedExecution = null"
      class="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4"
    >
      <div
        @click.stop
        class="bg-white rounded-lg shadow-xl max-w-2xl w-full max-h-[90vh] overflow-y-auto"
      >
        <div class="px-6 py-4 border-b border-gray-200 flex justify-between items-center">
          <h3 class="text-lg font-medium text-gray-900">Detalles de Ejecución</h3>
          <button
            @click="selectedExecution = null"
            class="text-gray-400 hover:text-gray-600"
          >
            <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>
        <div class="px-6 py-4 space-y-4">
          <div class="grid grid-cols-2 gap-4">
            <div>
              <span class="text-sm font-medium text-gray-500">ID:</span>
              <p class="text-sm text-gray-900 font-mono">{{ selectedExecution.execution_id }}</p>
            </div>
            <div>
              <span class="text-sm font-medium text-gray-500">Tipo:</span>
              <p class="text-sm text-gray-900">{{ selectedExecution.execution_type }}</p>
            </div>
            <div>
              <span class="text-sm font-medium text-gray-500">Entidad:</span>
              <p class="text-sm text-gray-900">{{ selectedExecution.entity }}</p>
            </div>
            <div>
              <span class="text-sm font-medium text-gray-500">Estado:</span>
              <span :class="getStatusClass(selectedExecution.status)" class="px-2 py-1 text-xs rounded-full">
                {{ getStatusLabel(selectedExecution.status) }}
              </span>
            </div>
            <div>
              <span class="text-sm font-medium text-gray-500">Registros procesados:</span>
              <p class="text-sm text-gray-900">{{ formatNumber(selectedExecution.records_processed) }}</p>
            </div>
            <div>
              <span class="text-sm font-medium text-gray-500">Registros insertados:</span>
              <p class="text-sm text-gray-900">{{ formatNumber(selectedExecution.records_inserted) }}</p>
            </div>
            <div>
              <span class="text-sm font-medium text-gray-500">Registros fallidos:</span>
              <p class="text-sm text-red-600">{{ formatNumber(selectedExecution.records_failed) }}</p>
            </div>
            <div>
              <span class="text-sm font-medium text-gray-500">Duración:</span>
              <p class="text-sm text-gray-900">{{ formatDuration(selectedExecution.elapsed_time) }}</p>
            </div>
          </div>
          <div v-if="selectedExecution.error_message" class="mt-4 p-3 bg-red-50 border border-red-200 rounded">
            <p class="text-sm font-medium text-red-800">Error:</p>
            <p class="text-sm text-red-700 mt-1">{{ selectedExecution.error_message }}</p>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useETLStore } from '@/stores/etl'

const route = useRoute()
const router = useRouter()
const etlStore = useETLStore()

const selectedExecution = ref(null)

// Filtros
const filter = ref('all')
const typeFilter = ref('all')
const currentPage = ref(1)
const pageSize = ref(50)

// Modos de visualización
const isActiveMode = computed(() => filter.value === 'active')
const isHistoryMode = computed(() => filter.value === 'history')

// Usar datos del store con reactividad
const loading = computed(() => etlStore.loading)

// Filtrar ejecuciones según el filtro seleccionado
const executions = computed(() => {
  let data = []

  // Determinar fuente de datos según filtro
  if (filter.value === 'active') {
    // Mostrar solo procesos activos
    data = etlStore.activeProcesses
  } else if (filter.value === 'history') {
    // Mostrar solo finalizadas (no running)
    data = etlStore.recentExecutions.filter(e => e.status !== 'running')
  } else if (filter.value === 'all') {
    // Combinar activos y recientes, sin duplicados
    const activeIds = new Set(etlStore.activeProcesses.map(p => p.execution_id))
    data = [
      ...etlStore.activeProcesses,
      ...etlStore.recentExecutions.filter(e => !activeIds.has(e.execution_id))
    ]
  } else {
    // Filtros específicos (completed, failed, cancelled)
    data = etlStore.recentExecutions.filter(e => e.status === filter.value)
  }

  // Aplicar filtro de tipo si es necesario
  if (typeFilter.value !== 'all') {
    data = data.filter(e => e.execution_type === typeFilter.value)
  }

  return data
})

// Stats calculadas desde el store
const totalCount = computed(() => {
  const stats = etlStore.statistics.total_executions
  return stats?.total || 0
})

const activeCount = computed(() => {
  return etlStore.activeProcesses.length
})

const completedCount = computed(() => {
  const stats = etlStore.statistics.total_executions
  return stats?.completed || 0
})

const failedCount = computed(() => {
  const stats = etlStore.statistics.total_executions
  return stats?.failed || 0
})

onMounted(async () => {
  // Si viene con query param filter, aplicar
  if (route.query.filter) {
    filter.value = route.query.filter
  }

  // Cargar datos del store
  await etlStore.loadAll()

  // Conectar WebSocket para actualizaciones en tiempo real
  etlStore.connectWebSocket()
})

onUnmounted(() => {
  // Desconectar WebSocket al salir
  etlStore.disconnectWebSocket()
})

// Watch for route changes
watch(() => route.query.filter, (newFilter) => {
  if (newFilter) {
    filter.value = newFilter
  }
})

function onFilterChange() {
  // Actualizar URL query param cuando cambia el filtro
  router.push({ query: { filter: filter.value } })
}

function viewDetails(execution) {
  selectedExecution.value = execution
}

async function cancelExecution(executionId) {
  if (!confirm('¿Estás seguro de que quieres cancelar esta ejecución?')) {
    return
  }

  try {
    await etlStore.stopExecution(executionId)
  } catch (error) {
    console.error('Error canceling execution:', error)
    alert('Error al cancelar la ejecución')
  }
}

function nextPage() {
  currentPage.value++
}

function previousPage() {
  if (currentPage.value > 1) {
    currentPage.value--
  }
}

// Utilidades
function getStatusClass(status) {
  const classes = {
    running: 'bg-blue-100 text-blue-800',
    completed: 'bg-green-100 text-green-800',
    failed: 'bg-red-100 text-red-800',
    cancelled: 'bg-gray-100 text-gray-800',
    pending: 'bg-yellow-100 text-yellow-800'
  }
  return classes[status] || 'bg-gray-100 text-gray-800'
}

function getStatusLabel(status) {
  const labels = {
    running: 'En ejecución',
    completed: 'Completado',
    failed: 'Fallido',
    cancelled: 'Cancelado',
    pending: 'Pendiente'
  }
  return labels[status] || status
}

function getProgressColor(status) {
  if (status === 'completed') return 'bg-green-500'
  if (status === 'failed') return 'bg-red-500'
  if (status === 'running') return 'bg-blue-500'
  return 'bg-gray-500'
}

function formatNumber(num) {
  if (!num && num !== 0) return '0'
  return new Intl.NumberFormat('es-ES').format(num)
}

function formatDateTime(dateString) {
  if (!dateString) return 'N/A'
  return new Date(dateString).toLocaleString('es-ES', {
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit'
  })
}

function formatDuration(seconds) {
  if (!seconds) return 'N/A'

  const hours = Math.floor(seconds / 3600)
  const minutes = Math.floor((seconds % 3600) / 60)
  const secs = seconds % 60

  if (hours > 0) {
    return `${hours}h ${minutes}m ${secs}s`
  } else if (minutes > 0) {
    return `${minutes}m ${secs}s`
  } else {
    return `${secs}s`
  }
}
</script>
