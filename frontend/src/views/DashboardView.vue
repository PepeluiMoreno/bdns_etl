<template>
  <div class="space-y-6">
    <!-- Panel de Alertas/Incidencias -->
    <div v-if="alerts.length > 0" class="bg-red-50 border-l-4 border-red-500 rounded-lg p-4">
      <div class="flex items-start">
        <div class="flex-shrink-0">
          <svg class="h-6 w-6 text-red-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
          </svg>
        </div>
        <div class="ml-3 flex-1">
          <h3 class="text-sm font-medium text-red-800">Incidencias que requieren atención</h3>
          <div class="mt-2 text-sm text-red-700">
            <ul class="list-disc list-inside space-y-1">
              <li v-for="(alert, index) in alerts" :key="index">
                {{ alert.message }}
                <button
                  v-if="alert.action"
                  @click="alert.action()"
                  class="ml-2 text-red-900 underline hover:text-red-600"
                >
                  {{ alert.actionLabel }}
                </button>
              </li>
            </ul>
          </div>
        </div>
      </div>
    </div>

    <!-- Stats Cards Grid -->
    <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
      <!-- Historial Procesos -->
      <div
        @click="goToExecutions"
        class="bg-white rounded-lg shadow p-6 cursor-pointer hover:shadow-lg hover:bg-gray-50 transition-all duration-200 transform hover:-translate-y-1"
      >
        <div class="flex items-center justify-between">
          <div class="text-sm font-medium text-gray-600">Historial</div>
          <svg class="w-4 h-4 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7" />
          </svg>
        </div>
        <div class="mt-2 text-3xl font-bold text-gray-900">{{ stats.total_executions?.total || 0 }}</div>
        <p class="mt-1 text-xs text-gray-500">Procesos finalizados</p>
        <div class="mt-3 flex items-center text-xs">
          <span class="text-green-600">{{ stats.total_executions?.completed || 0 }} OK</span>
          <span class="mx-2">•</span>
          <span class="text-red-600">{{ stats.total_executions?.failed || 0 }} fallos</span>
        </div>
      </div>

      <!-- Procesos Activos -->
      <div
        @click="goToActiveProcesses"
        class="bg-white rounded-lg shadow p-6 cursor-pointer hover:shadow-lg hover:bg-gray-50 transition-all duration-200 transform hover:-translate-y-1"
      >
        <div class="flex items-center justify-between">
          <div class="text-sm font-medium text-gray-600">Procesos Activos</div>
          <svg class="w-4 h-4 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7" />
          </svg>
        </div>
        <div class="mt-2 flex items-center">
          <div class="text-3xl font-bold text-green-600">{{ stats.active_processes || 0 }}</div>
          <div v-if="stats.active_processes > 0" class="ml-3">
            <span class="relative flex h-3 w-3">
              <span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75"></span>
              <span class="relative inline-flex rounded-full h-3 w-3 bg-green-500"></span>
            </span>
          </div>
        </div>
        <p class="mt-1 text-xs text-gray-500">En ejecución ahora</p>
      </div>

      <!-- Registros Totales -->
      <div class="bg-white rounded-lg shadow p-6">
        <div class="text-sm font-medium text-gray-600">Registros Totales</div>
        <div class="mt-2 text-3xl font-bold text-blue-600">{{ formatNumber(stats.total_records || 0) }}</div>
        <p class="mt-1 text-xs text-gray-500">En base de datos</p>
        <div class="mt-3 text-xs text-gray-500">
          Último seeding: {{ formatDate(stats.last_successful?.seeding) }}
        </div>
      </div>

      <!-- Tasa de Éxito -->
      <div class="bg-white rounded-lg shadow p-6">
        <div class="text-sm font-medium text-gray-600">Tasa de Éxito</div>
        <div class="mt-2 text-3xl font-bold" :class="getSuccessRateClass(stats.success_rate)">
          {{ stats.success_rate || 0 }}%
        </div>
        <p class="mt-1 text-xs text-gray-500">Últimas 30 ejecuciones</p>
        <div class="mt-3">
          <div class="w-full bg-gray-200 rounded-full h-2">
            <div
              class="h-2 rounded-full transition-all"
              :class="getSuccessRateClass(stats.success_rate, true)"
              :style="{ width: `${stats.success_rate || 0}%` }"
            ></div>
          </div>
        </div>
      </div>
    </div>

    <!-- Procesos en Ejecución -->
    <div v-if="activeProcesses.length > 0" class="bg-white rounded-lg shadow border-l-4 border-green-500">
      <div class="px-6 py-4 border-b border-gray-200 flex items-center justify-between">
        <div class="flex items-center">
          <span class="relative flex h-3 w-3 mr-3">
            <span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75"></span>
            <span class="relative inline-flex rounded-full h-3 w-3 bg-green-500"></span>
          </span>
          <h2 class="text-lg font-medium text-gray-900">
            Procesos en Ejecución ({{ activeProcesses.length }})
          </h2>
        </div>
        <button
          @click="router.push({ name: 'Processes', query: { filter: 'active' } })"
          class="text-sm text-blue-600 hover:text-blue-800"
        >
          Ver todos →
        </button>
      </div>
      <div class="p-6 space-y-4">
        <div
          v-for="process in activeProcesses"
          :key="process.execution_id"
          class="bg-gray-50 rounded-lg p-4 border border-gray-200"
        >
          <!-- Header del proceso -->
          <div class="flex items-start justify-between mb-3">
            <div class="flex-1">
              <div class="flex items-center">
                <span class="text-sm font-semibold text-gray-900">{{ process.entity }}</span>
                <span class="mx-2 text-gray-400">•</span>
                <span class="text-sm text-gray-600">{{ process.execution_type }}</span>
                <span v-if="process.year" class="mx-2 text-gray-400">•</span>
                <span v-if="process.year" class="text-sm text-gray-600">Año {{ process.year }}</span>
              </div>
              <div class="text-xs text-gray-500 mt-1">
                ID: {{ process.execution_id.substring(0, 8) }}
              </div>
            </div>
            <div class="text-right">
              <div class="text-2xl font-bold text-green-600">{{ process.progress || 0 }}%</div>
              <div class="text-xs text-gray-500">completado</div>
            </div>
          </div>

          <!-- Barra de progreso -->
          <div class="mb-3">
            <div class="w-full bg-gray-200 rounded-full h-3 overflow-hidden">
              <div
                class="h-full bg-gradient-to-r from-green-500 to-green-600 transition-all duration-500 ease-out"
                :style="{ width: `${process.progress || 0}%` }"
              ></div>
            </div>
          </div>

          <!-- Métricas -->
          <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
            <div>
              <div class="text-xs text-gray-500">Fase actual</div>
              <div class="text-sm font-medium text-gray-900">{{ process.current_phase || 'Procesando' }}</div>
            </div>
            <div>
              <div class="text-xs text-gray-500">Registros</div>
              <div class="text-sm font-medium text-gray-900">{{ formatNumber(process.records_processed || 0) }}</div>
            </div>
            <div>
              <div class="text-xs text-gray-500">Tiempo transcurrido</div>
              <div class="text-sm font-medium text-gray-900">{{ formatElapsedTime(process.started_at) }}</div>
            </div>
            <div>
              <div class="text-xs text-gray-500">Velocidad</div>
              <div class="text-sm font-medium text-gray-900">
                {{ formatSpeed(process.records_per_second) }} reg/s
              </div>
            </div>
          </div>

          <!-- Operación actual -->
          <div v-if="process.current_operation" class="mt-3 pt-3 border-t border-gray-200">
            <div class="text-xs text-gray-500 mb-1">Operación actual:</div>
            <div class="text-sm text-gray-700 font-mono truncate">{{ process.current_operation }}</div>
          </div>

          <!-- Botones de acción -->
          <div class="mt-3 pt-3 border-t border-gray-200 flex items-center justify-end space-x-2">
            <button
              @click="viewProcessDetails(process)"
              class="px-3 py-1 text-xs font-medium text-blue-700 bg-blue-50 hover:bg-blue-100 rounded transition-colors"
            >
              Ver detalles
            </button>
            <button
              @click="cancelProcess(process.execution_id)"
              class="px-3 py-1 text-xs font-medium text-red-700 bg-red-50 hover:bg-red-100 rounded transition-colors"
            >
              Cancelar
            </button>
          </div>
        </div>
      </div>
    </div>

    <!-- Matriz de Cobertura: Año x Entidad -->
    <div class="bg-white rounded-lg shadow">
      <div class="px-6 py-4 border-b border-gray-200">
        <h2 class="text-lg font-medium text-gray-900">Matriz de Cobertura de Datos</h2>
        <p class="text-sm text-gray-500">Registros y cobertura por año y entidad</p>
      </div>
      <div class="p-6">
        <div v-if="loading" class="text-center py-8 text-gray-500">
          Cargando cobertura...
        </div>
        <div v-else class="overflow-x-auto">
          <table class="min-w-full divide-y divide-gray-200">
            <thead>
              <tr>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider bg-gray-50">
                  Año / Entidad
                </th>
                <th
                  v-for="entity in coverage"
                  :key="entity.name"
                  class="px-6 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider bg-gray-50"
                >
                  {{ entity.label }}
                </th>
              </tr>
            </thead>
            <tbody class="bg-white divide-y divide-gray-200">
              <tr v-for="year in getYearsFromCoverage()" :key="year" class="hover:bg-gray-50">
                <td class="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                  {{ year }}
                </td>
                <td
                  v-for="entity in coverage"
                  :key="`${year}-${entity.name}`"
                  class="px-6 py-4 whitespace-nowrap text-center"
                >
                  <div class="flex flex-col items-center">
                    <div class="text-sm font-semibold text-gray-900">
                      {{ formatNumber(getRecordsForYearEntity(year, entity.name)) }}
                    </div>
                    <div
                      class="text-xs font-medium px-2 py-0.5 rounded-full mt-1"
                      :class="getCoverageClass(getCoverageForYearEntity(year, entity.name))"
                    >
                      {{ getCoverageForYearEntity(year, entity.name) }}%
                    </div>
                  </div>
                </td>
              </tr>
              <!-- Fila de totales -->
              <tr class="bg-gray-50 font-semibold">
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                  TOTAL
                </td>
                <td
                  v-for="entity in coverage"
                  :key="`total-${entity.name}`"
                  class="px-6 py-4 whitespace-nowrap text-center text-sm text-gray-900"
                >
                  {{ formatNumber(entity.records) }}
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>

    <!-- Procesos Recientes -->
    <div class="bg-white rounded-lg shadow">
      <div class="px-6 py-4 border-b border-gray-200 flex justify-between items-center">
        <h2 class="text-lg font-medium text-gray-900">Procesos Recientes</h2>
        <button
          @click="router.push({ name: 'Processes', query: { filter: 'history' } })"
          class="text-sm text-blue-600 hover:text-blue-800"
        >
          Ver todas →
        </button>
      </div>
      <div class="p-6">
        <div v-if="loading" class="text-center py-8 text-gray-500">
          Cargando...
        </div>
        <div v-else-if="recentExecutions.length === 0" class="text-center py-8 text-gray-500">
          No hay ejecuciones registradas
        </div>
        <div v-else class="overflow-x-auto">
          <table class="min-w-full divide-y divide-gray-200">
            <thead>
              <tr>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Tipo
                </th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Entidad
                </th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Estado
                </th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Registros
                </th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Fecha
                </th>
              </tr>
            </thead>
            <tbody class="bg-white divide-y divide-gray-200">
              <tr v-for="execution in recentExecutions" :key="execution.execution_id" class="hover:bg-gray-50">
                <td class="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                  {{ execution.execution_type }}
                </td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                  {{ execution.entity }}
                </td>
                <td class="px-6 py-4 whitespace-nowrap">
                  <span :class="getStatusClass(execution.status)" class="px-2 py-1 text-xs rounded-full">
                    {{ getStatusLabel(execution.status) }}
                  </span>
                </td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                  {{ formatNumber(execution.records_processed) }}
                </td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                  {{ formatDate(execution.started_at) }}
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted, onUnmounted, computed } from 'vue'
import { useRouter } from 'vue-router'
import { useETLStore } from '@/stores/etl'
import axios from 'axios'

const router = useRouter()
const etlStore = useETLStore()

const coverage = ref([])
const alerts = ref([])
const loading = ref(true)

// Usar datos del store
const stats = computed(() => etlStore.statistics)
const recentExecutions = computed(() => etlStore.recentExecutions)
const activeProcesses = computed(() => etlStore.activeProcesses)

onMounted(async () => {
  // Cargar datos iniciales
  await etlStore.loadAll()
  loadCoverage()
  loadAlerts()

  // Conectar WebSocket para actualizaciones en tiempo real
  etlStore.connectWebSocket()

  loading.value = false
})

onUnmounted(() => {
  // Desconectar WebSocket al salir
  etlStore.disconnectWebSocket()
})

// Navegación
function goToExecutions() {
  router.push({ name: 'Processes', query: { filter: 'history' } })
}

function goToActiveProcesses() {
  router.push({ name: 'Processes', query: { filter: 'active' } })
}

async function loadCoverage() {
  try {
    const response = await axios.get('/api/etl/coverage')
    coverage.value = response.data.entities || [
      { name: 'convocatorias', label: 'Convocatorias', coverage: 85, records: 150000, years: [2020, 2021, 2022, 2023, 2024] },
      { name: 'concesiones', label: 'Concesiones', coverage: 92, records: 450000, years: [2019, 2020, 2021, 2022, 2023, 2024] },
      { name: 'beneficiarios', label: 'Beneficiarios', coverage: 78, records: 280000, years: [2020, 2021, 2022, 2023] },
      { name: 'catalogos', label: 'Catálogos', coverage: 100, records: 5000, years: ['N/A'] }
    ]
  } catch (error) {
    console.error('Error loading coverage:', error)
    // Datos de ejemplo si falla la API
    coverage.value = [
      { name: 'convocatorias', label: 'Convocatorias', coverage: 85, records: 150000, years: [2020, 2021, 2022, 2023, 2024] },
      { name: 'concesiones', label: 'Concesiones', coverage: 92, records: 450000, years: [2019, 2020, 2021, 2022, 2023, 2024] },
      { name: 'beneficiarios', label: 'Beneficiarios', coverage: 78, records: 280000, years: [2020, 2021, 2022, 2023] },
      { name: 'catalogos', label: 'Catálogos', coverage: 100, records: 5000, years: ['N/A'] }
    ]
  }
}

async function loadAlerts() {
  try {
    const response = await axios.get('/api/etl/alerts')
    alerts.value = response.data || []
  } catch (error) {
    console.error('Error loading alerts:', error)
    // Generar alertas basadas en el store
    const failedCount = etlStore.statistics.total_executions?.failed || 0
    if (failedCount > 0) {
      alerts.value = [{
        message: `Hay ${failedCount} proceso(s) fallido(s) que requieren atención`,
        actionLabel: 'Reintentar',
        action: retryFailed
      }]
    }
  }
}

function viewProcessDetails(process) {
  router.push({ name: 'Processes', query: { filter: 'active' } })
}

async function cancelProcess(executionId) {
  if (!confirm('¿Estás seguro de que quieres cancelar este proceso?')) {
    return
  }

  try {
    await etlStore.stopExecution(executionId)
  } catch (error) {
    console.error('Error canceling process:', error)
    alert('Error al cancelar el proceso')
  }
}

async function retryFailed() {
  if (!confirm('¿Reintentar todos los procesos fallidos?')) {
    return
  }

  try {
    await axios.post('/api/etl/retry-failed')
    alert('Procesos fallidos reintentados con éxito')
    await etlStore.loadAll()
    loadAlerts()
  } catch (error) {
    console.error('Error retrying failed:', error)
    alert('Error al reintentar procesos fallidos')
  }
}

// Utilidades
function formatDate(dateString) {
  if (!dateString) return 'N/A'
  return new Date(dateString).toLocaleString('es-ES', {
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit'
  })
}

function formatNumber(num) {
  if (!num && num !== 0) return '0'
  return new Intl.NumberFormat('es-ES').format(num)
}

function getStatusClass(status) {
  const classes = {
    completed: 'bg-green-100 text-green-800',
    running: 'bg-blue-100 text-blue-800',
    failed: 'bg-red-100 text-red-800',
    cancelled: 'bg-gray-100 text-gray-800'
  }
  return classes[status] || 'bg-gray-100 text-gray-800'
}

function getStatusLabel(status) {
  const labels = {
    running: 'En ejecución',
    completed: 'Completado',
    failed: 'Fallido',
    cancelled: 'Cancelado'
  }
  return labels[status] || status
}

function getSuccessRateClass(rate, isBg = false) {
  if (rate >= 90) return isBg ? 'bg-green-500' : 'text-green-600'
  if (rate >= 70) return isBg ? 'bg-yellow-500' : 'text-yellow-600'
  return isBg ? 'bg-red-500' : 'text-red-600'
}

function getEntityColor(coverage) {
  if (coverage >= 90) return 'bg-green-500'
  if (coverage >= 70) return 'bg-yellow-500'
  return 'bg-red-500'
}

function getCoverageClass(coverage) {
  if (coverage >= 90) return 'bg-green-100 text-green-800'
  if (coverage >= 70) return 'bg-yellow-100 text-yellow-800'
  return 'bg-red-100 text-red-800'
}

function getCoverageBarClass(coverage) {
  if (coverage >= 90) return 'bg-green-500'
  if (coverage >= 70) return 'bg-yellow-500'
  return 'bg-red-500'
}

function formatElapsedTime(startedAt) {
  if (!startedAt) return 'N/A'

  const start = new Date(startedAt)
  const now = new Date()
  const elapsed = Math.floor((now - start) / 1000) // segundos

  const hours = Math.floor(elapsed / 3600)
  const minutes = Math.floor((elapsed % 3600) / 60)
  const seconds = elapsed % 60

  if (hours > 0) {
    return `${hours}h ${minutes}m`
  } else if (minutes > 0) {
    return `${minutes}m ${seconds}s`
  } else {
    return `${seconds}s`
  }
}

function formatSpeed(recordsPerSecond) {
  if (!recordsPerSecond && recordsPerSecond !== 0) return '0'
  if (recordsPerSecond >= 1000) {
    return `${(recordsPerSecond / 1000).toFixed(1)}k`
  }
  return Math.round(recordsPerSecond).toString()
}

// Funciones para la matriz de cobertura
function getYearsFromCoverage() {
  if (!coverage.value || coverage.value.length === 0) return []

  // Extraer todos los años únicos de todas las entidades
  const allYears = new Set()
  coverage.value.forEach(entity => {
    if (entity.years && Array.isArray(entity.years)) {
      entity.years.forEach(year => {
        if (year !== 'N/A') allYears.add(year)
      })
    }
  })

  // Ordenar años de más reciente a más antiguo
  return Array.from(allYears).sort((a, b) => b - a)
}

function getRecordsForYearEntity(year, entityName) {
  // Por ahora retornar datos simulados
  // TODO: Implementar consulta real a la API
  const entity = coverage.value.find(e => e.name === entityName)
  if (!entity || !entity.years.includes(year)) return 0

  // Simulación basada en el total de registros
  return Math.floor(entity.records / entity.years.length)
}

function getCoverageForYearEntity(year, entityName) {
  // Por ahora retornar datos simulados
  // TODO: Implementar consulta real a la API
  const entity = coverage.value.find(e => e.name === entityName)
  if (!entity || !entity.years.includes(year)) return 0

  return entity.coverage
}
</script>
