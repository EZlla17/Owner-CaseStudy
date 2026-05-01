"use client"

import Image from "next/image"
import { useMemo, useState } from "react"
import {
  Bar,
  BarChart,
  CartesianGrid,
  ResponsiveContainer,
  XAxis,
  YAxis,
} from "recharts"
import {
  Briefcase,
  Building2,
  ChefHat,
  CircleDashed,
  Lightbulb,
  RotateCw,
  Users,
} from "lucide-react"

import { Avatar, AvatarFallback } from "@/components/ui/avatar"
import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Separator } from "@/components/ui/separator"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"

type ChartPoint = {
  name: string
  calls: number
  successRate: number
}

type SituationPoint = {
  clusterId: number
  name: string
  count: number
  proceedRate: number
  suggestedStrategies?: {
    strategy: string
    count: number
    proceedRate: number
  }[]
  strategiesToAvoid?: {
    strategy: string
    count: number
    proceedRate: number
  }[]
}

type DashboardData = {
  kpis: {
    transcriptsAnalyzed: number
    demoBookedRate: number
    demoBookedCount: number
  }
  charts: {
    repTenureOutcome: ChartPoint[]
    restaurantTypeOutcome: ChartPoint[]
    cuisineTypeOutcome: ChartPoint[]
  }
  situations: SituationPoint[]
}

type RestaurantPoint = {
  id: string
  name: string
  cuisine: string
  location: string
  rating: number | null
  hasOnlineOrdering: boolean | null
  hasPickup: boolean | null
  hasCatering: boolean | null
  description: string
  positiveReviewPoints: string
  negativeReviewPoints: string
  firstOrderPlatform: string
  allOrderPlatforms: string
  fitScore: number | null
}

function labelCase(value: string) {
  return value
    .replaceAll("_", " ")
    .split(" ")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ")
}

function OutcomeBarChart({
  title,
  subtitle,
  data,
  icon: Icon,
}: {
  title: string
  subtitle: string
  data: ChartPoint[]
  icon: React.ComponentType<{ className?: string }>
}) {
  const chartData = data.map((item) => ({ ...item, label: labelCase(item.name) }))
  const plotHeight = Math.max(150, chartData.length * 32)
  const cardHeight = plotHeight + 86

  return (
    <Card className="bg-white/90 shadow-sm ring-1 ring-slate-200" style={{ height: cardHeight }}>
      <CardHeader className="pb-1">
        <CardTitle className="flex items-center gap-2 text-base font-semibold text-slate-800">
          <Icon className="h-4 w-4 text-slate-500" />
          {title}
        </CardTitle>
        <p className="text-xs text-slate-500">{subtitle}</p>
      </CardHeader>
      <CardContent className="pt-0" style={{ height: plotHeight }}>
        <ResponsiveContainer width="100%" height="100%">
          <BarChart
            data={chartData}
            layout="vertical"
            margin={{ left: 28, right: 12, top: 6, bottom: 4 }}
            barCategoryGap="42%"
          >
            <CartesianGrid horizontal={false} strokeDasharray="3 3" stroke="#e2e8f0" />
            <XAxis type="number" domain={[0, 100]} tick={{ fill: "#64748b", fontSize: 11 }} tickFormatter={(v) => `${v}%`} />
            <YAxis
              dataKey="label"
              type="category"
              width={120}
              interval={0}
              tickMargin={12}
              tick={{ fill: "#475569", fontSize: 11 }}
              tickLine={false}
              axisLine={false}
            />
            <Bar dataKey="successRate" radius={6} barSize={14} fill="#00842A" />
          </BarChart>
        </ResponsiveContainer>
      </CardContent>
    </Card>
  )
}

export function DashboardView({
  data,
  restaurants,
}: {
  data: DashboardData
  restaurants: RestaurantPoint[]
}) {
  const [activeTab, setActiveTab] = useState<"insights" | "restaurants">("insights")
  const [searchTerm, setSearchTerm] = useState("")
  const [rankBy, setRankBy] = useState<"proceed_asc" | "frequency_desc">(
    "frequency_desc"
  )
  const [restaurantSearchTerm, setRestaurantSearchTerm] = useState("")
  const [fitScoreFilter, setFitScoreFilter] = useState<
    "all" | "very_strong" | "strong" | "medium" | "weak"
  >("all")
  const [cuisineFilter, setCuisineFilter] = useState<string>("all")
  const [ratingFilter, setRatingFilter] = useState<
    "all" | "high" | "good" | "fair" | "low_or_na"
  >("all")
  const [selectedRestaurantId, setSelectedRestaurantId] = useState<string>(
    restaurants[0]?.id ?? ""
  )

  const visibleSituations = useMemo(() => {
    const filtered = data.situations.filter((item) =>
      item.name.toLowerCase().includes(searchTerm.trim().toLowerCase())
    )
    return filtered.sort((a, b) => {
      if (rankBy === "proceed_asc") return a.proceedRate - b.proceedRate
      return b.count - a.count
    })
  }, [data.situations, rankBy, searchTerm])

  const cuisineOptions = useMemo(() => {
    return Array.from(
      new Set(
        restaurants
          .map((restaurant) => (restaurant.cuisine || "unknown").trim())
          .filter(Boolean)
      )
    ).sort((a, b) => a.localeCompare(b))
  }, [restaurants])

  const visibleRestaurants = useMemo(() => {
    const query = restaurantSearchTerm.trim().toLowerCase()
    const filtered = restaurants.filter((restaurant) => {
      const matchesSearch =
        !query ||
        restaurant.name.toLowerCase().includes(query) ||
        restaurant.id.toLowerCase().includes(query) ||
        restaurant.location.toLowerCase().includes(query)

      const score = restaurant.fitScore ?? -1
      const matchesFitScore =
        fitScoreFilter === "all" ||
        (fitScoreFilter === "very_strong" && score >= 80) ||
        (fitScoreFilter === "strong" && score >= 60 && score < 80) ||
        (fitScoreFilter === "medium" && score >= 40 && score < 60) ||
        (fitScoreFilter === "weak" && score < 40)

      const cuisineValue = (restaurant.cuisine || "unknown").trim().toLowerCase()
      const matchesCuisine =
        cuisineFilter === "all" || cuisineValue === cuisineFilter.toLowerCase()

      const rating = restaurant.rating
      const matchesRating =
        ratingFilter === "all" ||
        (ratingFilter === "high" && rating !== null && rating >= 4.5) ||
        (ratingFilter === "good" && rating !== null && rating >= 4.0 && rating < 4.5) ||
        (ratingFilter === "fair" && rating !== null && rating >= 3.5 && rating < 4.0) ||
        (ratingFilter === "low_or_na" && (rating === null || rating < 3.5))

      return matchesSearch && matchesFitScore && matchesCuisine && matchesRating
    })
    return filtered.sort((a, b) => {
      const scoreA = a.fitScore ?? -1
      const scoreB = b.fitScore ?? -1
      if (scoreA !== scoreB) return scoreB - scoreA
      const ratingA = a.rating ?? -1
      const ratingB = b.rating ?? -1
      if (ratingA !== ratingB) return ratingB - ratingA
      return a.name.localeCompare(b.name)
    })
  }, [restaurants, restaurantSearchTerm, fitScoreFilter, cuisineFilter, ratingFilter])

  const selectedRestaurant = useMemo(
    () =>
      restaurants.find((restaurant) => restaurant.id === selectedRestaurantId) ??
      visibleRestaurants[0] ??
      null,
    [restaurants, selectedRestaurantId, visibleRestaurants]
  )

  const splitReviewPoints = (raw: string) =>
    raw
      .split(",")
      .map((x) => x.trim())
      .filter(Boolean)

  const platformLabel = (raw: string) => {
    const value = raw.trim().toLowerCase()
    if (!value || value === "unknown") return "Unknown"
    if (value === "restaurant_own") return "Own platform"
    return value
      .split("_")
      .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
      .join(" ")
  }

  const allPlatformList = (restaurant: RestaurantPoint) => {
    const fromAll = restaurant.allOrderPlatforms
      .split(",")
      .map((x) => x.trim())
      .filter(Boolean)
    const unique = Array.from(new Set(fromAll))
    const first = (restaurant.firstOrderPlatform || "").trim()
    if (!first || first === "unknown") return unique
    return [first, ...unique.filter((platform) => platform !== first)]
  }

  return (
    <div className="h-screen overflow-hidden bg-white text-slate-900">
      <header className="fixed inset-x-0 top-0 z-40 w-full border-b border-slate-200 bg-white px-6 py-3 md:px-8">
        <div className="flex items-center justify-between">
          <div className="flex items-center">
            <Image src="/owner-logo.png" alt="Owner logo" width={180} height={52} className="h-9 w-auto" priority />
          </div>
          <div className="flex items-center gap-2">
            <Avatar className="h-8 w-8 bg-slate-900">
              <AvatarFallback className="bg-slate-900 text-xs text-white">SM</AvatarFallback>
            </Avatar>
            <span className="text-sm text-slate-700">Sales Manager</span>
          </div>
        </div>
      </header>

      <div className="mt-[3.75rem] h-[calc(100vh-3.75rem)] px-4 py-4 md:px-5 md:py-4">
        <div className="mx-auto flex h-full max-w-[1480px] flex-col gap-4">
        <div className="flex h-full gap-6">
          <aside className="fixed top-[calc(3.75rem+1rem)] bottom-4 hidden w-36 shrink-0 rounded-2xl bg-neutral-100 p-3 md:flex md:flex-col">
            <div className="mt-2 space-y-2 text-left text-xs text-slate-500">
              <button
                type="button"
                onClick={() => setActiveTab("insights")}
                className={`flex w-full items-center gap-1.5 rounded-lg px-2 py-2 text-left ${
                  activeTab === "insights"
                    ? "bg-white font-medium text-slate-800"
                    : "text-slate-500"
                }`}
              >
                <Lightbulb className="h-3.5 w-3.5" />
                Insights
              </button>
              <button
                type="button"
                onClick={() => setActiveTab("restaurants")}
                className={`flex w-full items-center gap-1.5 rounded-lg px-2 py-2 text-left ${
                  activeTab === "restaurants"
                    ? "bg-white font-medium text-slate-800"
                    : "text-slate-500"
                }`}
              >
                <Building2 className="h-3.5 w-3.5" />
                Restaurants
              </button>
              <div className="flex items-center gap-1.5 rounded-lg px-2 py-2">
                <CircleDashed className="h-3.5 w-3.5" />
                Placeholder
              </div>
              <div className="flex items-center gap-1.5 rounded-lg px-2 py-2">
                <CircleDashed className="h-3.5 w-3.5" />
                Placeholder
              </div>
            </div>
            <div className="mt-auto px-1 py-3 text-center text-[11px] text-slate-500">Help</div>
          </aside>

          <main className="h-full flex-1 md:pl-[10.5rem]">
          {activeTab === "insights" ? (
          <div className="h-full overflow-hidden rounded-2xl bg-neutral-100 p-4 md:p-5">
          <div className="hide-scrollbar h-full overflow-y-auto px-2 pt-2 pb-3">
          <div className="grid h-full items-stretch gap-7 xl:grid-cols-[1.03fr_1.35fr]">
            <section className="h-full space-y-7 pb-1">
              <div className="grid gap-7 sm:grid-cols-2">
                <Card className="bg-white shadow-sm ring-1 ring-slate-200">
                  <CardHeader className="pb-0">
                    <CardTitle className="flex items-center justify-between text-base font-semibold text-slate-700">
                      <span>Transcripts Analyzed</span>
                      <RotateCw className="h-4 w-4 text-slate-400" />
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="mt-1 text-5xl font-semibold tracking-tight text-slate-900">
                      {data.kpis.transcriptsAnalyzed}
                    </div>
                  </CardContent>
                </Card>
                <Card className="bg-white shadow-sm ring-1 ring-slate-200">
                  <CardHeader className="pb-0">
                    <CardTitle className="text-base font-semibold text-slate-700">Demo Booked Rate</CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="mt-1 text-5xl font-semibold tracking-tight text-slate-900">
                      {data.kpis.demoBookedRate.toFixed(1)}%
                    </div>
                    <p className="mt-2 text-xs text-slate-500">{data.kpis.demoBookedCount} / {data.kpis.transcriptsAnalyzed} booked demos</p>
                  </CardContent>
                </Card>
              </div>

              <OutcomeBarChart
                title="Rep Tenure · Outcome"
                subtitle="Success rate by rep tenure segment"
                data={data.charts.repTenureOutcome}
                icon={Users}
              />
              <OutcomeBarChart
                title="Restaurant Type · Outcome"
                subtitle="Success rate by restaurant type"
                data={data.charts.restaurantTypeOutcome}
                icon={Briefcase}
              />
              <OutcomeBarChart
                title="Cuisine Type · Outcome"
                subtitle="Success rate by cuisine type"
                data={data.charts.cuisineTypeOutcome}
                icon={ChefHat}
              />
            </section>

            <section className="h-full min-h-0 pb-1">
              <Card className="flex h-full min-h-0 flex-col bg-white shadow-sm ring-1 ring-slate-200">
                <CardHeader className="pb-3">
                  <CardTitle className="text-xl font-semibold text-slate-700">Key Situations</CardTitle>
                </CardHeader>
                <CardContent className="min-h-0 flex flex-1 flex-col pt-0">
                  <div className="mb-4 flex items-center gap-2">
                    <Input
                      placeholder="Search situations..."
                      value={searchTerm}
                      onChange={(e) => setSearchTerm(e.target.value)}
                      className="h-9 border-neutral-300 bg-white"
                    />
                    <Select
                      value={rankBy}
                      onValueChange={(value) =>
                        setRankBy(value as "proceed_asc" | "frequency_desc")
                      }
                    >
                      <SelectTrigger className="h-9 min-w-44 bg-white">
                        <SelectValue placeholder="Rank by" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="proceed_asc">
                          Rank by Proceed Rate (low to high)
                        </SelectItem>
                        <SelectItem value="frequency_desc">
                          Rank by Frequency (high to low)
                        </SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                  <div className="hide-scrollbar min-h-0 flex-1 overflow-y-auto pr-2 pb-2">
                    <div className="space-y-4">
                      {visibleSituations.map((item) => (
                        <div
                          key={item.clusterId}
                          className="rounded-2xl border border-neutral-200 bg-neutral-50 px-4 py-4"
                        >
                          <div className="flex items-center justify-between gap-4">
                            <div className="min-w-0 space-y-1">
                              <p className="truncate text-[15px] font-semibold text-slate-800">{item.name}</p>
                              <p className="text-[11px] tracking-[0.08em] text-slate-500 uppercase">
                                {item.count} moments
                              </p>
                            </div>
                            <div className="flex flex-col items-end justify-center text-right">
                              <p className="text-[10px] font-medium tracking-[0.16em] text-slate-500 uppercase">
                                Proceed Rate
                              </p>
                              <p className="text-[18px] font-semibold tracking-tight text-slate-900">
                                {item.proceedRate.toFixed(1)}%
                              </p>
                            </div>
                          </div>

                          {(() => {
                            const allStrategies = [
                              ...(item.suggestedStrategies ?? []),
                              ...(item.strategiesToAvoid ?? []),
                            ]
                            const deduped = Array.from(
                              new Map(
                                allStrategies.map((s) => [
                                  s.strategy,
                                  s,
                                ])
                              ).values()
                            ).sort((a, b) => b.proceedRate - a.proceedRate)

                            if (deduped.length === 0) return null

                            return (
                            <div className="mt-3 border-t border-neutral-200 pt-2.5">
                              <p className="mb-1 text-[13px] font-semibold tracking-[0.02em] text-black">
                                How to proceed
                              </p>
                              <div className="space-y-1.5">
                                {deduped.map((s, idx) => {
                                  const shouldHighlight = idx < 3 && s.proceedRate > 50
                                  return (
                                  <div key={`strategy-${item.clusterId}-${s.strategy}`} className="flex items-center justify-between gap-3 text-[13px]">
                                    <p
                                      className={`line-clamp-1 ${
                                        shouldHighlight ? "font-medium text-[#00842A]" : "text-slate-700"
                                      }`}
                                    >
                                      {s.strategy}
                                    </p>
                                    <span
                                      className={`shrink-0 font-medium ${
                                        shouldHighlight ? "text-[#00842A]" : "text-slate-800"
                                      }`}
                                    >
                                      {s.proceedRate.toFixed(1)}%
                                    </span>
                                  </div>
                                )})}
                              </div>
                            </div>
                            )
                          })()}
                        </div>
                      ))}
                      {visibleSituations.length === 0 && (
                        <div className="rounded-2xl border border-dashed border-neutral-300 bg-neutral-50 px-4 py-8 text-center text-sm text-slate-500">
                          No situations matched your search.
                        </div>
                      )}
                    </div>
                  </div>
                </CardContent>
              </Card>
            </section>
          </div>
          </div>
          </div>
          ) : (
          <div className="h-full">
            <div className="grid h-full min-h-0 gap-7 xl:grid-cols-[0.92fr_1.46fr]">
              <section className="h-full min-h-0 pb-1">
                <div className="flex h-full min-h-0 flex-col bg-transparent">
                  <div className="min-h-0 flex flex-1 flex-col pt-0">
                    <div className="mb-4 space-y-2">
                      <Input
                        placeholder="Search restaurants..."
                        value={restaurantSearchTerm}
                        onChange={(e) => setRestaurantSearchTerm(e.target.value)}
                        className="h-9 border-neutral-300 bg-white"
                      />
                      <div className="grid gap-2 sm:grid-cols-3">
                        <Select
                          value={fitScoreFilter}
                          onValueChange={(value) =>
                            setFitScoreFilter(
                              value as "all" | "very_strong" | "strong" | "medium" | "weak"
                            )
                          }
                        >
                          <SelectTrigger className="h-9 bg-white">
                            <SelectValue placeholder="Filter by fit score" />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="all">Fit Score: All</SelectItem>
                            <SelectItem value="very_strong">Fit Score: 80+</SelectItem>
                            <SelectItem value="strong">Fit Score: 60-79</SelectItem>
                            <SelectItem value="medium">Fit Score: 40-59</SelectItem>
                            <SelectItem value="weak">Fit Score: &lt; 40</SelectItem>
                          </SelectContent>
                        </Select>
                        <Select
                          value={cuisineFilter}
                          onValueChange={(value) => setCuisineFilter(value)}
                        >
                          <SelectTrigger className="h-9 bg-white">
                            <SelectValue placeholder="Filter by cuisine type" />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="all">Cuisine: All</SelectItem>
                            {cuisineOptions.map((cuisine) => (
                              <SelectItem key={`cuisine-${cuisine}`} value={cuisine}>
                                {cuisine}
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                        <Select
                          value={ratingFilter}
                          onValueChange={(value) =>
                            setRatingFilter(
                              value as "all" | "high" | "good" | "fair" | "low_or_na"
                            )
                          }
                        >
                          <SelectTrigger className="h-9 bg-white">
                            <SelectValue placeholder="Filter by rating" />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="all">Rating: All</SelectItem>
                            <SelectItem value="high">Rating: 4.5+</SelectItem>
                            <SelectItem value="good">Rating: 4.0-4.4</SelectItem>
                            <SelectItem value="fair">Rating: 3.5-3.9</SelectItem>
                            <SelectItem value="low_or_na">Rating: &lt; 3.5 or N/A</SelectItem>
                          </SelectContent>
                        </Select>
                      </div>
                    </div>
                    <div className="hide-scrollbar min-h-0 flex-1 overflow-y-auto pr-1 pb-2">
                      <div className="space-y-2">
                        {visibleRestaurants.map((restaurant) => (
                          <div
                            key={restaurant.id}
                            className={`cursor-pointer rounded-xl border px-3 py-2.5 transition-colors ${
                              restaurant.id === selectedRestaurant?.id
                                ? "border-[#00842A] bg-green-50/40"
                                : "border-neutral-200 bg-neutral-50 hover:bg-neutral-100"
                            }`}
                            onClick={() => setSelectedRestaurantId(restaurant.id)}
                          >
                            <div className="flex items-center justify-between gap-3">
                              <div className="min-w-0">
                                <p className="truncate text-[14px] font-medium text-slate-800">
                                  {restaurant.name}
                                </p>
                                <div className="mt-0.5 flex items-center gap-3 text-[11px] text-slate-600">
                                  <p className="truncate">{restaurant.location || "Location unavailable"}</p>
                                  <p className="shrink-0 font-medium text-slate-500">
                                    Rating:{" "}
                                    <span className="text-[#00842A]">
                                      {restaurant.rating !== null ? restaurant.rating.toFixed(1) : "N/A"}
                                    </span>
                                  </p>
                                </div>
                              </div>
                              <div className="shrink-0 self-center text-right">
                                <p className="text-[10px] tracking-[0.12em] text-slate-500 uppercase">
                                  Fit Score
                                </p>
                                <p className="text-[18px] font-semibold leading-5 text-slate-900">
                                  {restaurant.fitScore !== null ? restaurant.fitScore.toFixed(0) : "N/A"}
                                </p>
                              </div>
                            </div>
                          </div>
                        ))}
                        {visibleRestaurants.length === 0 && (
                          <div className="rounded-xl border border-dashed border-neutral-300 bg-neutral-50 px-3 py-6 text-center text-sm text-slate-500">
                            No restaurants matched your search.
                          </div>
                        )}
                      </div>
                    </div>
                  </div>
                </div>
              </section>
              <section className="h-full min-h-0 pb-1">
                <Card className="flex h-full min-h-0 flex-col bg-white shadow-sm ring-1 ring-slate-200">
                  <CardHeader className="pb-0" />
                  <CardContent className="hide-scrollbar min-h-0 flex flex-1 flex-col gap-4 overflow-y-auto px-7 pt-0 md:px-8">
                    {selectedRestaurant ? (
                      <>
                        <div className="relative space-y-1.5">
                          <div className="absolute top-0 right-0 flex shrink-0 flex-col items-end gap-1.5">
                            <div className="text-right">
                              <p className="text-[11px] tracking-[0.08em] text-slate-500 uppercase">Fit Score</p>
                              <p className="text-3xl font-semibold tracking-tight text-slate-900">
                                {selectedRestaurant.fitScore !== null
                                  ? selectedRestaurant.fitScore.toFixed(0)
                                  : "N/A"}
                              </p>
                            </div>
                          </div>
                          <p className="pr-44 text-2xl font-semibold tracking-tight text-slate-900">
                              {selectedRestaurant.name}
                          </p>
                          <div className="flex items-center gap-4 pt-0.5 text-sm text-slate-600">
                            <span>{selectedRestaurant.location || "Location unavailable"}</span>
                            <Badge variant="secondary" className="bg-white text-slate-700 ring-1 ring-neutral-200">
                              Rating{" "}
                              <span className="ml-1 font-semibold text-[#00842A]">
                                {selectedRestaurant.rating !== null
                                  ? selectedRestaurant.rating.toFixed(1)
                                  : "N/A"}
                              </span>
                            </Badge>
                          </div>
                          <div className="flex flex-wrap items-center gap-2 pt-0.5 text-sm text-slate-700">
                            <Badge variant="outline" className="rounded-full border-[#00842A] bg-green-50 text-slate-700">
                              Cuisine: {selectedRestaurant.cuisine || "unknown"}
                            </Badge>
                          </div>
                        </div>
                        <Separator className="my-4" />
                        <div className="space-y-2 rounded-2xl border border-neutral-200 bg-neutral-50 p-4">
                          <p className="text-base font-semibold text-slate-900">Description</p>
                          <p className="text-sm leading-7 text-slate-600">
                            {selectedRestaurant.description || "No description available."}
                          </p>
                        </div>
                        <div className="space-y-3 pt-4">
                          <p className="text-base font-semibold text-slate-900">Notable reviews</p>
                          <div className="grid gap-4 md:grid-cols-2">
                            <Card className="h-full border-neutral-200 bg-neutral-50 shadow-none">
                              <CardHeader className="pb-0">
                                <CardTitle className="text-sm text-slate-800">Positive reviews</CardTitle>
                              </CardHeader>
                              <CardContent className="space-y-0.5 pt-0.5">
                                {splitReviewPoints(selectedRestaurant.positiveReviewPoints).length > 0 ? (
                                  splitReviewPoints(selectedRestaurant.positiveReviewPoints).map((point) => (
                                    <p key={`pos-${selectedRestaurant.id}-${point}`} className="text-sm leading-6 text-slate-600">
                                      {point}
                                    </p>
                                  ))
                                ) : (
                                  <p className="text-sm text-slate-400">No notable positive reviews.</p>
                                )}
                              </CardContent>
                            </Card>
                            <Card className="h-full border-neutral-200 bg-neutral-50 shadow-none">
                              <CardHeader className="pb-0">
                                <CardTitle className="text-sm text-slate-800">Negative reviews</CardTitle>
                              </CardHeader>
                              <CardContent className="space-y-0.5 pt-0.5">
                                {splitReviewPoints(selectedRestaurant.negativeReviewPoints).length > 0 ? (
                                  splitReviewPoints(selectedRestaurant.negativeReviewPoints).map((point) => (
                                    <p key={`neg-${selectedRestaurant.id}-${point}`} className="text-sm leading-6 text-slate-600">
                                      {point}
                                    </p>
                                  ))
                                ) : (
                                  <p className="text-sm text-slate-400">No notable negative reviews.</p>
                                )}
                              </CardContent>
                            </Card>
                          </div>
                        </div>
                        <div className="space-y-3 pt-2">
                          <p className="text-base font-semibold text-slate-900">Ordering setup</p>
                          <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
                            {allPlatformList(selectedRestaurant).length > 0 ? (
                              allPlatformList(selectedRestaurant).map((platform) => {
                                const isFirst =
                                  selectedRestaurant.firstOrderPlatform &&
                                  selectedRestaurant.firstOrderPlatform !== "unknown" &&
                                  platform === selectedRestaurant.firstOrderPlatform
                                const platformIndex = allPlatformList(selectedRestaurant).indexOf(platform) + 1
                                return (
                                  <div
                                    key={`platform-${selectedRestaurant.id}-${platform}`}
                                    className={`rounded-xl px-3 py-2.5 text-sm font-medium ${
                                      isFirst
                                        ? "border border-[#00842A] bg-green-50 text-slate-800"
                                        : "border border-neutral-200 bg-neutral-50 text-slate-700"
                                    }`}
                                  >
                                    <span className="mr-1 text-slate-500">
                                      {platformIndex}.
                                    </span>
                                    <span>{platformLabel(platform)}</span>
                                  </div>
                                )
                              })
                            ) : (
                              <p className="text-sm text-slate-400">No ordering platform detected.</p>
                            )}
                          </div>
                        </div>
                      </>
                    ) : (
                      <p className="text-sm text-slate-400">No restaurant selected</p>
                    )}
                  </CardContent>
                </Card>
              </section>
            </div>
          </div>
          )}
          </main>
        </div>
      </div>
      </div>
    </div>
  )
}
