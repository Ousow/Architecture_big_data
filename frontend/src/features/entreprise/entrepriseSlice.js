import { createSlice, createAsyncThunk } from '@reduxjs/toolkit'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000'

export const loadEntreprise = createAsyncThunk('entreprise/load', async (numero) => {
  const res = await fetch(`${API_URL}/entreprise/${numero}`)
  if (!res.ok) throw new Error('Entreprise introuvable')
  return res.json()
})

const entrepriseSlice = createSlice({
  name: 'entreprise',
  initialState: {
    data: null,
    selectedYear: null,
    status: 'idle',
  },
  reducers: {
    clearEntreprise(state) {
      state.data = null
      state.selectedYear = null
      state.status = 'idle'
    },
    setSelectedYear(state, action) {
      state.selectedYear = action.payload
    },
  },
  extraReducers: (builder) => {
    builder
      .addCase(loadEntreprise.pending, (state) => {
        state.status = 'loading'
      })
      .addCase(loadEntreprise.fulfilled, (state, action) => {
        state.status = 'succeeded'
        state.data = action.payload
        const years = action.payload.financials?.years || []
        state.selectedYear = years.length ? years[years.length - 1].year : null
      })
      .addCase(loadEntreprise.rejected, (state) => {
        state.status = 'failed'
        state.data = null
      })
  },
})

export const { clearEntreprise, setSelectedYear } = entrepriseSlice.actions
export default entrepriseSlice.reducer
