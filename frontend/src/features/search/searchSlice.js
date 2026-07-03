import { createSlice, createAsyncThunk } from '@reduxjs/toolkit'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000'

export const runSearch = createAsyncThunk('search/run', async (query) => {
  const res = await fetch(`${API_URL}/search?q=${encodeURIComponent(query)}`)
  if (!res.ok) throw new Error('Erreur de recherche')
  return res.json()
})

const searchSlice = createSlice({
  name: 'search',
  initialState: {
    query: '',
    results: [],
    status: 'idle', // idle | loading | succeeded | failed
  },
  reducers: {
    setQuery(state, action) {
      state.query = action.payload
    },
    clearResults(state) {
      state.results = []
      state.status = 'idle'
    },
  },
  extraReducers: (builder) => {
    builder
      .addCase(runSearch.pending, (state) => {
        state.status = 'loading'
      })
      .addCase(runSearch.fulfilled, (state, action) => {
        state.status = 'succeeded'
        state.results = action.payload.results
      })
      .addCase(runSearch.rejected, (state) => {
        state.status = 'failed'
        state.results = []
      })
  },
})

export const { setQuery, clearResults } = searchSlice.actions
export default searchSlice.reducer
