<template>
  <div>
    <!-- Stats compactas -->
    <div class="bg-white border-b border-gray-200 px-6 py-3 mb-6 -mx-6 -mt-6">
      <div class="flex items-center gap-4 text-xs">
        <div class="flex items-center gap-1.5">
          <span class="text-gray-500">Total:</span>
          <span class="font-semibold text-gray-900">{{ totalCount }}</span>
        </div>
        <div class="w-px h-4 bg-gray-300"></div>
        <div class="flex items-center gap-1.5">
          <span class="text-gray-500">Activas:</span>
          <span class="font-semibold text-green-600">{{ activeCount }}</span>
        </div>
        <div class="w-px h-4 bg-gray-300"></div>
        <div class="flex items-center gap-1.5">
          <span class="text-gray-500">Completadas:</span>
          <span class="font-semibold text-blue-600">{{ completedCount }}</span>
        </div>
        <div class="w-px h-4 bg-gray-300"></div>
        <div class="flex items-center gap-1.5">
          <span class="text-gray-500">Fallidas:</span>
          <span class="font-semibold text-red-600">{{ failedCount }}</span>
        </div>
      </div>
    </div>

    <!-- Panel de filtros -->
    <div class="mb-6">
      <PanelFiltrosProcesos
        :filtros="filtros"
        @aplicar="aplicarFiltros"
        @limpiar="limpiarFiltros"
      />
    </div>

    <!-- Lista de procesos -->
    <div class="bg-white rounded-lg shadow">
      <!-- Loading state -->
      <div v-if="loading" class="p-12 text-center">
        <div class="inline-block animate-spin rounded-full h-8 w-8 border-4 border-blue-500 border-t-transparent"></div>
        <p class="mt-4 text-gray-600">Cargando procesos...</p>
      </div>

      <!-- Empty state -->
      <div v-else-if="executions.length === 0" class="p-12 text-center">
        <svg class="mx-auto h-12 w-12 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
        </svg>
        <p class="mt-4 text-gray-600">No hay procesos que coincidan con el filtro</p>
      </div>

      <!-- Tabla de procesos -->
      <div v-else class="overflow-x-auto">
        <table class="min-w-full divide-y divide-gray-200">
          <thead class="bg-gray-50">
            <tr>
              <th class="px-3 py-2 text-left text-sm font-medium text-gray-500">
                Entidad
              </th>
              <th class="px-3 py-2 text-left text-sm font-medium text-gray-500">
                Progreso
              </th>
              <th class="px-3 py-2 text-left text-sm font-medium text-gray-500">
                Registros
              </th>
              <th class="px-3 py-2 text-left text-sm font-medium text-gray-500">
                Inicio
              </th>
              <th class="px-3 py-2 text-left text-sm font-medium text-gray-500">
                Finalización
              </th>
              <th class="px-3 py-2 text-left text-sm font-medium text-gray-500">
                Duración
              </th>
              <th class="px-3 py-2 text-left text-sm font-medium text-gray-500">
                Estado
              </th>
              <th class="px-3 py-2 text-left text-sm font-medium text-gray-500">
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
              <!-- Entidad -->
              <td class="px-3 py-2 whitespace-nowrap">
                <div class="text-xs font-medium text-gray-900">
                  {{ formatEntityName(execution.entity, execution.year) }}
                </div>
              </td>

              <!-- Progreso -->
              <td class="px-3 py-2 whitespace-nowrap">
                <div class="w-20">
                  <div class="flex items-center">
                    <div class="flex-1">
                      <div class="w-full bg-gray-200 rounded-full h-1.5">
                        <div
                          :class="getProgressColor(execution.status)"
                          class="h-1.5 rounded-full transition-all"
                          :style="{ width: `${execution.progress || 0}%` }"
                        ></div>
                      </div>
                    </div>
                    <span class="ml-1 text-xs text-gray-600">{{ execution.progress || 0 }}%</span>
                  </div>
                </div>
              </td>

              <!-- Registros -->
              <td class="px-3 py-2 whitespace-nowrap text-xs text-gray-500">
                <div>{{ formatNumber(execution.records_processed || 0) }}</div>
                <div v-if="execution.records_failed > 0" class="text-xs text-red-600">
                  {{ execution.records_failed }} err
                </div>
              </td>

              <!-- Inicio -->
              <td class="px-3 py-2 whitespace-nowrap text-xs text-gray-500">
                {{ formatDateTime(execution.started_at) }}
              </td>

              <!-- Finalización -->
              <td class="px-3 py-2 whitespace-nowrap text-xs text-gray-500">
                {{ formatDateTime(execution.finished_at) }}
              </td>

              <!-- Duración -->
              <td class="px-3 py-2 whitespace-nowrap text-xs text-gray-500">
                {{ formatDuration(execution.elapsed_time) }}
              </td>

              <!-- Estado -->
              <td class="px-3 py-2 whitespace-nowrap">
                <span :class="getStatusClass(execution.status)" class="px-2 py-0.5 text-xs font-medium rounded-full">
                  {{ getStatusLabel(execution.status) }}
                </span>
              </td>

              <!-- Acciones -->
              <td class="px-3 py-2 whitespace-nowrap text-xs font-medium">
                <button
                  @click="viewDetails(execution)"
                  class="text-blue-600 hover:text-blue-900 mr-2"
                  title="Ver detalles"
                >
                  Ver
                </button>
                <button
                  v-if="execution.status === 'running'"
                  @click="openCancelModal(execution)"
                  class="text-red-600 hover:text-red-900 mr-2"
                  title="Cancelar"
                >
                  Cancelar
                </button>
                <button
                  v-if="['cancelled', 'failed', 'interrupted'].includes(execution.status)"
                  @click="restartExecution(execution)"
                  class="text-green-600 hover:text-green-900 mr-2"
                  :title="execution.status === 'failed' ? 'Reintentar' : execution.status === 'interrupted' ? 'Relanzar' : 'Reiniciar'"
                >
                  {{ execution.status === 'failed' ? 'Reintentar' : execution.status === 'interrupted' ? 'Relanzar' : 'Reiniciar' }}
                </button>
                <button
                  v-if="execution.status !== 'running'"
                  @click="openDeleteModal(execution)"
                  class="text-red-600 hover:text-red-900"
                  title="Eliminar"
                >
                  Eliminar
                </button>
              </td>
            </tr>
          </tbody>
        </table>
      </div>

      <!-- Paginación -->
      <div v-if="executions.length > 0" class="px-6 py-4 border-t border-gray-200 flex items-center justify-between">
        <div class="text-sm text-gray-700">
          Mostrando {{ executions.length }} de {{ totalCount }} procesos
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
      @click="closeModal"
      class="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4"
    >
      <div
        @click.stop
        class="bg-white rounded-lg shadow-xl max-w-4xl w-full max-h-[90vh] overflow-hidden flex flex-col"
      >
        <!-- Header -->
        <div class="px-6 py-4 border-b border-gray-200 flex justify-between items-center bg-gray-50">
          <div>
            <h3 class="text-lg font-semibold text-gray-900">
              {{ formatEntityName(selectedExecution.entity, selectedExecution.year) }}
            </h3>
          </div>
          <div class="flex items-center gap-2">
            <span
              v-if="selectedExecution.status === 'running'"
              class="flex items-center gap-1 text-xs text-green-600 font-medium"
            >
              <span class="inline-block w-2 h-2 bg-green-600 rounded-full animate-pulse"></span>
              Actualizando en tiempo real
            </span>
            <button
              @click="closeModal"
              class="text-gray-400 hover:text-gray-600 transition-colors"
            >
              <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
              </svg>
            </button>
          </div>
        </div>

        <!-- Info Grid -->
        <div class="px-6 py-4 grid grid-cols-4 gap-4 border-b border-gray-200">
          <!-- Estado -->
          <div>
            <div class="text-xs text-gray-500 mb-1">Estado</div>
            <span :class="getStatusClass(selectedExecution.status)" class="px-2 py-1 text-xs font-medium rounded-full inline-block">
              {{ getStatusLabel(selectedExecution.status) }}
            </span>
          </div>

          <!-- Progreso -->
          <div>
            <div class="text-xs text-gray-500 mb-1">Progreso</div>
            <div class="flex items-center gap-2">
              <div class="flex-1 bg-gray-200 rounded-full h-2">
                <div
                  :class="getProgressColor(selectedExecution.status)"
                  class="h-2 rounded-full"
                  :style="{ width: `${selectedExecution.progress || 0}%` }"
                ></div>
              </div>
              <span class="text-xs font-medium text-gray-700">{{ selectedExecution.progress || 0 }}%</span>
            </div>
          </div>

          <!-- Registros -->
          <div>
            <div class="text-xs text-gray-500 mb-1">Registros</div>
            <div class="text-sm font-medium text-gray-900">
              {{ formatNumber(selectedExecution.records_processed || 0) }}
            </div>
            <div v-if="selectedExecution.records_failed > 0" class="text-xs text-red-600">
              {{ selectedExecution.records_failed }} errores
            </div>
          </div>

          <!-- Duración -->
          <div>
            <div class="text-xs text-gray-500 mb-1">Duración</div>
            <div class="text-sm font-medium text-gray-900">
              {{ formatDuration(selectedExecution.elapsed_time) }}
            </div>
          </div>
        </div>

        <!-- Fechas -->
        <div class="px-6 py-3 grid grid-cols-2 gap-4 bg-gray-50 border-b border-gray-200">
          <div>
            <div class="text-xs text-gray-500 mb-1">Iniciado</div>
            <div class="text-sm text-gray-900">{{ formatDateTime(selectedExecution.started_at) }}</div>
          </div>
          <div>
            <div class="text-xs text-gray-500 mb-1">Finalizado</div>
            <div class="text-sm text-gray-900">{{ formatDateTime(selectedExecution.finished_at) }}</div>
          </div>
        </div>

        <!-- Log Terminal -->
        <div class="flex-1 flex flex-col overflow-hidden">
          <div class="px-6 py-2 bg-gray-800 text-gray-300 text-xs font-medium flex items-center justify-between">
            <div class="flex items-center gap-3">
              <span>Log de Ejecución</span>
              <button
                @click="logAutoScroll = !logAutoScroll"
                :class="logAutoScroll ? 'text-green-400' : 'text-gray-500'"
                class="hover:text-white transition-colors"
                :title="logAutoScroll ? 'Auto-scroll activado' : 'Auto-scroll desactivado'"
              >
                {{ logAutoScroll ? '📜 Auto' : '📜 Manual' }}
              </button>
            </div>
            <span class="text-gray-500">{{ selectedExecution.execution_id?.substring(0, 8) }}</span>
          </div>
          <div class="flex-1 bg-black text-yellow-400 font-mono text-xs p-4 overflow-y-auto log-container">
            <div v-if="selectedExecution.log" v-html="formatLog(selectedExecution.log)"></div>
            <div v-else class="text-gray-500 italic">
              [{{ new Date().toISOString() }}] Sin logs disponibles para este proceso<br>
              [{{ new Date().toISOString() }}] El proceso está {{ getStatusLabel(selectedExecution.status).toLowerCase() }}
            </div>
          </div>
        </div>

        <!-- Error message si existe -->
        <div v-if="selectedExecution.error_message" class="px-6 py-3 bg-red-900 text-red-100 border-t border-red-800">
          <div class="text-xs font-medium mb-1">ERROR</div>
          <div class="text-sm">{{ selectedExecution.error_message }}</div>
        </div>
      </div>
    </div>

    <!-- Modal de relanzamiento/reintento -->
    <div
      v-if="restartModalExecution"
      @click="restartModalExecution = null"
      class="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4"
    >
      <div
        @click.stop
        class="bg-white rounded-lg shadow-xl max-w-md w-full"
      >
        <div class="px-6 py-4 border-b border-gray-200">
          <h3 class="text-lg font-medium text-gray-900">
            {{ restartModalExecution.status === 'failed' ? 'Reintentar proceso' : 'Relanzar proceso' }}
          </h3>
        </div>
        <div class="px-6 py-4">
          <p class="text-sm text-gray-600 mb-4">
            <template v-if="restartModalExecution.status === 'failed'">
              ¿Reintentar <strong>{{ formatEntityName(restartModalExecution.entity, restartModalExecution.year) }}</strong>?
              Se volverá a ejecutar el proceso desde el principio.
            </template>
            <template v-else>
              ¿Relanzar <strong>{{ formatEntityName(restartModalExecution.entity, restartModalExecution.year) }}</strong>?
              Se reanudará desde el último punto de control.
            </template>
          </p>
          <label class="flex items-start gap-2 cursor-pointer">
            <input
              v-model="restartDeleteTemp"
              type="checkbox"
              class="mt-0.5 rounded border-gray-300 text-green-600 focus:ring-green-500"
            >
            <div>
              <span class="text-sm font-medium text-gray-900">Borrar archivos temporales</span>
              <p class="text-xs text-gray-500 mt-0.5">
                Elimina los CSV intermedios y JSON descargados. El proceso empezará desde cero.
              </p>
            </div>
          </label>
        </div>
        <div class="px-6 py-4 border-t border-gray-200 flex justify-end gap-3">
          <button
            @click="restartModalExecution = null"
            class="px-4 py-2 text-sm font-medium text-gray-700 hover:text-gray-900 border border-gray-300 rounded-md hover:bg-gray-50"
          >
            Cancelar
          </button>
          <button
            @click="confirmRestart"
            class="px-4 py-2 text-sm font-medium text-white bg-green-600 rounded-md hover:bg-green-700"
          >
            {{ restartModalExecution.status === 'failed' ? 'Reintentar' : 'Relanzar' }}
          </button>
        </div>
      </div>
    </div>

    <!-- Modal de cancelación -->
    <div
      v-if="cancelModalExecution"
      @click="cancelModalExecution = null"
      class="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4"
    >
      <div
        @click.stop
        class="bg-white rounded-lg shadow-xl max-w-md w-full"
      >
        <div class="px-6 py-4 border-b border-gray-200">
          <h3 class="text-lg font-medium text-gray-900">Cancelar proceso</h3>
        </div>
        <div class="px-6 py-4">
          <p class="text-sm text-gray-600 mb-4">
            ¿Cancelar <strong>{{ formatEntityName(cancelModalExecution.entity, cancelModalExecution.year) }}</strong>?
            El proceso se detendrá y quedará marcado como cancelado.
          </p>
          <label class="flex items-start gap-2 cursor-pointer">
            <input
              v-model="cancelDeleteTemp"
              type="checkbox"
              class="mt-0.5 rounded border-gray-300 text-red-600 focus:ring-red-500"
            >
            <div>
              <span class="text-sm font-medium text-gray-900">Borrar archivos temporales</span>
              <p class="text-xs text-gray-500 mt-0.5">
                Elimina los CSV intermedios y JSON descargados. La próxima ejecución empezará desde cero.
              </p>
            </div>
          </label>
        </div>
        <div class="px-6 py-4 border-t border-gray-200 flex justify-end gap-3">
          <button
            @click="cancelModalExecution = null"
            class="px-4 py-2 text-sm font-medium text-gray-700 hover:text-gray-900 border border-gray-300 rounded-md hover:bg-gray-50"
          >
            Cerrar
          </button>
          <button
            @click="confirmCancel"
            class="px-4 py-2 text-sm font-medium text-white bg-red-600 rounded-md hover:bg-red-700"
          >
            Cancelar proceso
          </button>
        </div>
      </div>
    </div>

    <!-- Modal de eliminación -->
    <div
      v-if="deleteModalExecution"
      @click="deleteModalExecution = null"
      class="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4"
    >
      <div
        @click.stop
        class="bg-white rounded-lg shadow-xl max-w-md w-full"
      >
        <div class="px-6 py-4 border-b border-gray-200">
          <h3 class="text-lg font-medium text-gray-900">Eliminar registro</h3>
        </div>
        <div class="px-6 py-4">
          <p class="text-sm text-gray-600 mb-4">
            ¿Eliminar el registro de <strong>{{ formatEntityName(deleteModalExecution.entity, deleteModalExecution.year) }}</strong> del historial?
          </p>
          <label class="flex items-start gap-2 cursor-pointer">
            <input
              v-model="deleteAlsoTemp"
              type="checkbox"
              class="mt-0.5 rounded border-gray-300 text-red-600 focus:ring-red-500"
            >
            <div>
              <span class="text-sm font-medium text-gray-900">Borrar también archivos temporales</span>
              <p class="text-xs text-gray-500 mt-0.5">
                Elimina los CSV intermedios y JSON descargados de esta entidad/ejercicio.
              </p>
            </div>
          </label>
        </div>
        <div class="px-6 py-4 border-t border-gray-200 flex justify-end gap-3">
          <button
            @click="deleteModalExecution = null"
            class="px-4 py-2 text-sm font-medium text-gray-700 hover:text-gray-900 border border-gray-300 rounded-md hover:bg-gray-50"
          >
            Cancelar
          </button>
          <button
            @click="confirmDelete"
            class="px-4 py-2 text-sm font-medium text-white bg-red-600 rounded-md hover:bg-red-700"
          >
            Eliminar
          </button>
        </div>
      </div>
    </div>

    <!-- Modal de error / información -->
    <div
      v-if="infoModal"
      @click="infoModal = null"
      class="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4"
    >
      <div
        @click.stop
        class="bg-white rounded-lg shadow-xl max-w-md w-full"
      >
        <div class="px-6 py-4 border-b border-gray-200">
          <h3 class="text-lg font-medium text-gray-900">{{ infoModal.title }}</h3>
        </div>
        <div class="px-6 py-4">
          <p class="text-sm text-gray-600">{{ infoModal.message }}</p>
        </div>
        <div class="px-6 py-4 border-t border-gray-200 flex justify-end">
          <button
            @click="infoModal = null"
            class="px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-md hover:bg-blue-700"
          >
            Aceptar
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted, watch } from 'vue'
import { useETLStore } from '@/stores/etl'
import PanelFiltrosProcesos from '@/components/PanelFiltrosProcesos.vue'

const etlStore = useETLStore()

const selectedExecution = ref(null)
const selectedExecutionId = ref(null)
const cancelModalExecution = ref(null)
const cancelDeleteTemp = ref(false)
const restartModalExecution = ref(null)
const restartDeleteTemp = ref(false)
const deleteModalExecution = ref(null)
const deleteAlsoTemp = ref(false)
const infoModal = ref(null)
const currentPage = ref(1)
const pageSize = ref(50)
const logAutoScroll = ref(true)

// Filtros
const filtros = ref({
  fechaDesde: null,
  fechaHasta: null,
  entidades: [],
  estadoEjecucion: 'todos',
  estadoTerminacion: 'todos'
})

// Filtros aplicados (los que realmente se usan para filtrar)
const filtrosAplicados = ref({
  fechaDesde: null,
  fechaHasta: null,
  entidades: [],
  estadoEjecucion: 'todos',
  estadoTerminacion: 'todos'
})

// Usar datos del store con reactividad
const loading = computed(() => etlStore.loading)

// Filtrar ejecuciones según filtros aplicados
const executions = computed(() => {
  let data = []

  // 1. Determinar fuente según estado de ejecución
  if (filtrosAplicados.value.estadosSeleccionados && filtrosAplicados.value.estadosSeleccionados.length > 0) {
    // Si hay estados seleccionados específicos
    const activeIds = new Set(etlStore.activeProcesses.map(p => p.execution_id))
    data = []

    if (filtrosAplicados.value.estadosSeleccionados.includes('running')) {
      data.push(...etlStore.activeProcesses)
    }
    if (filtrosAplicados.value.estadosSeleccionados.includes('finalizados')) {
      data.push(...etlStore.recentExecutions.filter(e => e.status !== 'running' && !activeIds.has(e.execution_id)))
    }
  } else if (filtrosAplicados.value.estadoEjecucion === 'running') {
    data = etlStore.activeProcesses
  } else if (filtrosAplicados.value.estadoEjecucion === 'finalizados') {
    data = etlStore.recentExecutions.filter(e => e.status !== 'running')
  } else {
    // 'todos': combinar activos y recientes sin duplicados
    const activeIds = new Set(etlStore.activeProcesses.map(p => p.execution_id))
    data = [
      ...etlStore.activeProcesses,
      ...etlStore.recentExecutions.filter(e => !activeIds.has(e.execution_id))
    ]
  }

  // 2. Filtrar por estado de terminación
  if (filtrosAplicados.value.resultadosSeleccionados && filtrosAplicados.value.resultadosSeleccionados.length > 0) {
    data = data.filter(e => filtrosAplicados.value.resultadosSeleccionados.includes(e.status))
  } else if (filtrosAplicados.value.estadoTerminacion !== 'todos') {
    data = data.filter(e => e.status === filtrosAplicados.value.estadoTerminacion)
  }

  // 3. Filtrar por entidades
  if (filtrosAplicados.value.entidades.length > 0) {
    data = data.filter(e => filtrosAplicados.value.entidades.includes(e.entity))
  }

  // 4. Filtrar por rango de fechas
  if (filtrosAplicados.value.fechaDesde) {
    const fechaDesde = new Date(filtrosAplicados.value.fechaDesde)
    data = data.filter(e => {
      if (!e.started_at) return false
      const fechaEjecucion = new Date(e.started_at)
      return fechaEjecucion >= fechaDesde
    })
  }

  if (filtrosAplicados.value.fechaHasta) {
    const fechaHasta = new Date(filtrosAplicados.value.fechaHasta)
    fechaHasta.setHours(23, 59, 59, 999) // Incluir todo el día
    data = data.filter(e => {
      if (!e.started_at) return false
      const fechaEjecucion = new Date(e.started_at)
      return fechaEjecucion <= fechaHasta
    })
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

function aplicarFiltros(nuevosFiltros) {
  filtrosAplicados.value = { ...nuevosFiltros }
}

function limpiarFiltros() {
  filtrosAplicados.value = {
    fechaDesde: null,
    fechaHasta: null,
    entidades: [],
    estadoEjecucion: 'todos',
    estadoTerminacion: 'todos'
  }
}

async function loadExecutions() {
  await etlStore.loadAll()
}

onMounted(async () => {
  // Cargar datos del store
  await etlStore.loadAll()

  // Conectar WebSocket para actualizaciones en tiempo real
  etlStore.connectWebSocket()
})

onUnmounted(() => {
  // Desconectar WebSocket al salir
  etlStore.disconnectWebSocket()
})

function viewDetails(execution) {
  selectedExecution.value = execution
  selectedExecutionId.value = execution.execution_id
}

function closeModal() {
  selectedExecution.value = null
  selectedExecutionId.value = null
  logAutoScroll.value = true
}

// Watch para actualizar selectedExecution en tiempo real
watch(
  () => [etlStore.activeProcesses, etlStore.recentExecutions],
  () => {
    if (selectedExecutionId.value) {
      // Buscar la ejecución actualizada en el store
      const updated = [
        ...etlStore.activeProcesses,
        ...etlStore.recentExecutions
      ].find(e => e.execution_id === selectedExecutionId.value)

      if (updated) {
        selectedExecution.value = updated

        // Auto-scroll al final del log si hay nueva información
        if (logAutoScroll.value) {
          setTimeout(() => {
            const logContainer = document.querySelector('.log-container')
            if (logContainer) {
              logContainer.scrollTop = logContainer.scrollHeight
            }
          }, 100)
        }
      }
    }
  },
  { deep: true }
)

function openCancelModal(execution) {
  cancelDeleteTemp.value = false
  cancelModalExecution.value = execution
}

async function confirmCancel() {
  const execution = cancelModalExecution.value
  const deleteTemp = cancelDeleteTemp.value
  cancelModalExecution.value = null

  try {
    await etlStore.stopExecution(execution.execution_id)

    if (deleteTemp) {
      await etlStore.cleanTempFiles(execution.entity, execution.year)
    }

    await etlStore.loadRecentExecutions()
  } catch (error) {
    console.error('Error cancelando ejecución:', error)
    infoModal.value = {
      title: 'Error al cancelar',
      message: error.response?.data?.detail || error.message
    }
  }
}

function restartExecution(execution) {
  restartDeleteTemp.value = false
  restartModalExecution.value = execution
}

async function confirmRestart() {
  const execution = restartModalExecution.value
  const deleteTemp = restartDeleteTemp.value
  restartModalExecution.value = null

  try {
    // Borrar archivos temporales si está marcado
    if (deleteTemp) {
      await etlStore.cleanTempFiles(execution.entity, execution.year)
    }

    await etlStore.startEntitySeeding(execution.entity, execution.year, execution.execution_id)
    await etlStore.loadRecentExecutions()
  } catch (error) {
    console.error('Error reiniciando ejecución:', error)
    infoModal.value = {
      title: 'No se puede iniciar el proceso',
      message: error.response?.data?.detail || error.message
    }
  }
}

function openDeleteModal(execution) {
  deleteAlsoTemp.value = false
  deleteModalExecution.value = execution
}

async function confirmDelete() {
  const execution = deleteModalExecution.value
  const alsoTemp = deleteAlsoTemp.value
  deleteModalExecution.value = null

  try {
    await etlStore.deleteExecution(execution.execution_id)

    if (alsoTemp) {
      await etlStore.cleanTempFiles(execution.entity, execution.year)
    }
  } catch (error) {
    console.error('Error eliminando ejecución:', error)
    infoModal.value = {
      title: 'Error al eliminar',
      message: error.response?.data?.detail || error.message
    }
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
    interrupted: 'bg-orange-100 text-orange-800',
    pending: 'bg-yellow-100 text-yellow-800',
    replaced: 'bg-gray-100 text-gray-500'
  }
  return classes[status] || 'bg-gray-100 text-gray-800'
}

function getStatusLabel(status) {
  const labels = {
    running: 'En ejecución',
    completed: 'Completado',
    failed: 'Fallido',
    cancelled: 'Cancelado',
    interrupted: 'Interrumpido',
    pending: 'Pendiente',
    replaced: 'Reemplazado'
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

function formatEntityName(entity, year) {
  const entityName = entity || 'N/A'

  // Capitalizar la primera letra de la entidad
  const capitalizedEntity = entityName.charAt(0).toUpperCase() + entityName.slice(1)

  // Los catálogos son atemporales, no llevan ejercicio
  if (entity === 'catalogos' || !year) return capitalizedEntity

  return `${capitalizedEntity} ${year}`
}

function formatLog(logText) {
  if (!logText) return ''

  // Escapar HTML para evitar XSS
  const escapeHtml = (text) => {
    const div = document.createElement('div')
    div.textContent = text
    return div.innerHTML
  }

  // Escapar el texto
  let formatted = escapeHtml(logText)

  // Convertir saltos de línea a <br>
  formatted = formatted.replace(/\n/g, '<br>')

  // Resaltar timestamps (formato ISO o similar)
  formatted = formatted.replace(
    /(\[?\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}(?:\.\d{3})?(?:Z|[+-]\d{2}:\d{2})?\]?)/g,
    '<span class="text-green-400">$1</span>'
  )

  // Resaltar errores y warnings
  formatted = formatted.replace(
    /(ERROR|FAILED|EXCEPTION|CRITICAL)/gi,
    '<span class="text-red-400 font-bold">$1</span>'
  )
  formatted = formatted.replace(
    /(WARNING|WARN)/gi,
    '<span class="text-orange-400 font-bold">$1</span>'
  )

  // Resaltar mensajes de éxito
  formatted = formatted.replace(
    /(SUCCESS|COMPLETED|OK|DONE)/gi,
    '<span class="text-green-400 font-bold">$1</span>'
  )

  // Resaltar INFO
  formatted = formatted.replace(
    /(INFO|DEBUG)/gi,
    '<span class="text-blue-400">$1</span>'
  )

  // Resaltar números y porcentajes
  formatted = formatted.replace(
    /\b(\d+(?:\.\d+)?%?)\b/g,
    '<span class="text-cyan-400">$1</span>'
  )

  return formatted
}
</script>
