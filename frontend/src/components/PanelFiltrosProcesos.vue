<template>
  <div class="bg-white rounded-lg shadow border border-gray-200">
    <!-- Línea 1: Fechas, Ejercicios y Estado -->
    <div class="px-4 py-2 flex items-center gap-6 text-xs border-b border-gray-200">
      <!-- Fechas de lanzamiento -->
      <div class="flex items-center gap-2">
        <span class="font-medium text-gray-700">Fechas de lanzamiento:</span>
        <input
          type="date"
          v-model="filtrosInternos.fechaDesde"
          class="px-2 py-1 border border-gray-300 rounded text-xs focus:outline-none focus:ring-1 focus:ring-blue-500"
        />
        <span class="text-gray-400">-</span>
        <input
          type="date"
          v-model="filtrosInternos.fechaHasta"
          class="px-2 py-1 border border-gray-300 rounded text-xs focus:outline-none focus:ring-1 focus:ring-blue-500"
        />
      </div>

      <div class="h-4 w-px bg-gray-300"></div>

      <!-- Ejercicios -->
      <div class="flex items-center gap-2">
        <span class="font-medium text-gray-700">Desde el ejercicio:</span>
        <input
          type="number"
          v-model.number="filtrosInternos.ejercicioDesde"
          placeholder="2015"
          min="2015"
          max="2030"
          class="px-2 py-1 border border-gray-300 rounded text-xs focus:outline-none focus:ring-1 focus:ring-blue-500 w-20"
        />
        <span class="font-medium text-gray-700">Hasta el ejercicio:</span>
        <input
          type="number"
          v-model.number="filtrosInternos.ejercicioHasta"
          placeholder="2025"
          min="2015"
          max="2030"
          class="px-2 py-1 border border-gray-300 rounded text-xs focus:outline-none focus:ring-1 focus:ring-blue-500 w-20"
        />
      </div>

      <div class="h-4 w-px bg-gray-300"></div>

      <!-- Estado Ejecución -->
      <div class="flex items-center gap-2">
        <span class="font-medium text-gray-700">Estado:</span>
        <label class="flex items-center gap-1 cursor-pointer">
          <input
            type="checkbox"
            :checked="todosEstadosSeleccionados"
            @change="toggleTodosEstados"
            class="w-3 h-3 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
          />
          <span class="text-gray-600">Todos</span>
        </label>
        <label class="flex items-center gap-1 cursor-pointer">
          <input
            type="checkbox"
            value="running"
            v-model="estadosSeleccionados"
            class="w-3 h-3 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
          />
          <span class="text-gray-600">Activos</span>
        </label>
        <label class="flex items-center gap-1 cursor-pointer">
          <input
            type="checkbox"
            value="finalizados"
            v-model="estadosSeleccionados"
            class="w-3 h-3 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
          />
          <span class="text-gray-600">Finalizados</span>
        </label>
      </div>
    </div>

    <!-- Línea 2: Entidades, Resultados y Botones -->
    <div class="px-4 py-2 flex items-center gap-6 text-xs">
      <!-- Entidades -->
      <div class="flex items-center gap-2">
        <span class="font-medium text-gray-700">Entidades:</span>
        <label class="flex items-center gap-1 cursor-pointer">
          <input
            type="checkbox"
            :checked="todasEntidadesSeleccionadas"
            @change="toggleTodasEntidades"
            class="w-3 h-3 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
          />
          <span class="text-gray-600">Todas</span>
        </label>
        <label
          v-for="entidad in entidadesDisponibles"
          :key="entidad.id"
          class="flex items-center gap-1 cursor-pointer"
        >
          <input
            type="checkbox"
            :value="entidad.id"
            v-model="filtrosInternos.entidades"
            class="w-3 h-3 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
          />
          <span class="text-gray-600">{{ entidad.nombre }}</span>
        </label>
      </div>

      <div class="h-4 w-px bg-gray-300"></div>

      <!-- Resultado -->
      <div class="flex items-center gap-2 flex-1">
        <span class="font-medium text-gray-700">Resultado:</span>
        <label class="flex items-center gap-1 cursor-pointer">
          <input
            type="checkbox"
            :checked="todosResultadosSeleccionados"
            @change="toggleTodosResultados"
            class="w-3 h-3 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
          />
          <span class="text-gray-600">Todos</span>
        </label>
        <label class="flex items-center gap-1 cursor-pointer">
          <input
            type="checkbox"
            value="completed"
            v-model="resultadosSeleccionados"
            class="w-3 h-3 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
          />
          <span class="text-gray-600">OK</span>
        </label>
        <label class="flex items-center gap-1 cursor-pointer">
          <input
            type="checkbox"
            value="failed"
            v-model="resultadosSeleccionados"
            class="w-3 h-3 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
          />
          <span class="text-gray-600">Error</span>
        </label>
        <label class="flex items-center gap-1 cursor-pointer">
          <input
            type="checkbox"
            value="cancelled"
            v-model="resultadosSeleccionados"
            class="w-3 h-3 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
          />
          <span class="text-gray-600">Cancelado</span>
        </label>
      </div>

      <!-- Botones -->
      <div class="flex gap-2 ml-auto">
        <button
          @click="limpiarFiltros"
          class="px-3 py-1 text-xs font-medium text-gray-700 bg-white border border-gray-300 rounded hover:bg-gray-50"
        >
          Limpiar
        </button>
        <button
          @click="aplicarFiltros"
          class="px-3 py-1 text-xs font-medium text-white bg-blue-600 rounded hover:bg-blue-700"
        >
          Aplicar
        </button>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, reactive, computed, watch } from 'vue'

const props = defineProps({
  filtros: {
    type: Object,
    default: () => ({
      fechaDesde: null,
      fechaHasta: null,
      ejercicioDesde: null,
      ejercicioHasta: null,
      entidades: [],
      estadoEjecucion: 'todos',
      estadoTerminacion: 'todos'
    })
  }
})

const emit = defineEmits(['aplicar', 'limpiar'])

const entidadesDisponibles = [
  { id: 'catalogos', nombre: 'Catálogos' },
  { id: 'convocatorias', nombre: 'Convocatorias' },
  { id: 'concesiones', nombre: 'Concesiones' },
  { id: 'minimis', nombre: 'De Minimis' },
  { id: 'ayudas_estado', nombre: 'Ayudas de Estado' },
  { id: 'partidos_politicos', nombre: 'Partidos Políticos' }
  // TODO: grandes_beneficiarios - deshabilitado temporalmente
  // { id: 'grandes_beneficiarios', nombre: 'Grandes Beneficiarios' }
]

const estadosDisponibles = ['running', 'finalizados']
const resultadosDisponibles = ['completed', 'failed', 'cancelled']

const filtrosInternos = reactive({
  fechaDesde: props.filtros.fechaDesde,
  fechaHasta: props.filtros.fechaHasta,
  ejercicioDesde: props.filtros.ejercicioDesde,
  ejercicioHasta: props.filtros.ejercicioHasta,
  entidades: [...(props.filtros.entidades || [])],
  estadoEjecucion: props.filtros.estadoEjecucion || 'todos',
  estadoTerminacion: props.filtros.estadoTerminacion || 'todos'
})

const estadosSeleccionados = ref([])
const resultadosSeleccionados = ref([])

const todasEntidadesSeleccionadas = computed(() => {
  return filtrosInternos.entidades.length === entidadesDisponibles.length
})

const todosEstadosSeleccionados = computed(() => {
  return estadosSeleccionados.value.length === 0 || estadosSeleccionados.value.length === estadosDisponibles.length
})

const todosResultadosSeleccionados = computed(() => {
  return resultadosSeleccionados.value.length === 0 || resultadosSeleccionados.value.length === resultadosDisponibles.length
})

function toggleTodasEntidades() {
  if (todasEntidadesSeleccionadas.value) {
    filtrosInternos.entidades = []
  } else {
    filtrosInternos.entidades = entidadesDisponibles.map(e => e.id)
  }
}

function toggleTodosEstados() {
  if (todosEstadosSeleccionados.value) {
    estadosSeleccionados.value = []
  } else {
    estadosSeleccionados.value = [...estadosDisponibles]
  }
}

function toggleTodosResultados() {
  if (todosResultadosSeleccionados.value) {
    resultadosSeleccionados.value = []
  } else {
    resultadosSeleccionados.value = [...resultadosDisponibles]
  }
}

function aplicarFiltros() {
  // Convertir estados seleccionados
  let estadoEjecucion = 'todos'
  if (estadosSeleccionados.value.length === 1) {
    estadoEjecucion = estadosSeleccionados.value[0]
  } else if (estadosSeleccionados.value.length > 0 && estadosSeleccionados.value.length < estadosDisponibles.length) {
    estadoEjecucion = 'multiples'
  }

  // Convertir resultados seleccionados
  let estadoTerminacion = 'todos'
  if (resultadosSeleccionados.value.length === 1) {
    estadoTerminacion = resultadosSeleccionados.value[0]
  } else if (resultadosSeleccionados.value.length > 0 && resultadosSeleccionados.value.length < resultadosDisponibles.length) {
    estadoTerminacion = 'multiples'
  }

  emit('aplicar', {
    ...filtrosInternos,
    estadoEjecucion,
    estadoTerminacion,
    estadosSeleccionados: estadosSeleccionados.value,
    resultadosSeleccionados: resultadosSeleccionados.value
  })
}

function limpiarFiltros() {
  filtrosInternos.fechaDesde = null
  filtrosInternos.fechaHasta = null
  filtrosInternos.ejercicioDesde = null
  filtrosInternos.ejercicioHasta = null
  filtrosInternos.entidades = []
  filtrosInternos.estadoEjecucion = 'todos'
  filtrosInternos.estadoTerminacion = 'todos'
  estadosSeleccionados.value = []
  resultadosSeleccionados.value = []
  emit('limpiar')
}
</script>
